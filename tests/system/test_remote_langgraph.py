from __future__ import annotations

import time

import httpx


GRAPH_ID = "demo_graph_v1"


def test_st_rlg_1_orchestrator_health(orchestrator_url: str) -> None:
    r = httpx.get(f"{orchestrator_url}/health", timeout=5.0)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "ok"
    assert body["redis"] == "ok"


def test_st_rlg_2_nodes_registered(orchestrator_url: str) -> None:
    r1 = httpx.get(f"{orchestrator_url}/resolve/node/{GRAPH_ID}/research", timeout=5.0)
    assert r1.status_code == 200, r1.text
    assert r1.json()["endpoint"].endswith("/invoke")

    r2 = httpx.get(f"{orchestrator_url}/resolve/node/{GRAPH_ID}/writer", timeout=5.0)
    assert r2.status_code == 200, r2.text
    assert r2.json()["endpoint"].endswith("/invoke")


def test_st_rlg_3_graph_manifest(orchestrator_url: str, run_main_langgraph) -> None:
    # Run the full demo — multiple scenarios register different topologies
    # under the same graph_id; the last compile() wins.  We verify the API
    # contract (correct status code, valid JSON shape) rather than a specific
    # node set, because the node list changes as more scenarios are added.
    result = run_main_langgraph()
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"

    r = httpx.get(f"{orchestrator_url}/graphs/{GRAPH_ID}", timeout=5.0)
    assert r.status_code == 200, r.text
    manifest = r.json()
    assert manifest["graph_id"] == GRAPH_ID
    assert isinstance(manifest["nodes"], list), "nodes should be a list"
    assert len(manifest["nodes"]) >= 2, f"expected at least 2 nodes, got: {manifest['nodes']}"
    assert isinstance(manifest["edges"], list), "edges should be a list"
    # The S1 nodes are always registered (first scenario always runs)
    # and any subsequent compile() adds its nodes.  Verify at least the
    # S1 remote nodes were seen by the orchestrator's resolve endpoint:
    r_res = httpx.get(f"{orchestrator_url}/resolve/node/{GRAPH_ID}/research", timeout=5.0)
    assert r_res.status_code == 200, "research node should still be resolvable"
    r_wri = httpx.get(f"{orchestrator_url}/resolve/node/{GRAPH_ID}/writer", timeout=5.0)
    assert r_wri.status_code == 200, "writer node should still be resolvable"


def test_st_rlg_4_end_to_end_run(orchestrator_url: str, jaeger_url: str, run_main_langgraph) -> None:
    result = run_main_langgraph()
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"
    # S1 (original research→writer flow) always runs first and must succeed.
    # The demo now uses structured logger output instead of a bare FINAL RESULT print.
    assert "✓ S1: ok" in result.stdout, (
        f"S1 scenario did not report ok in demo output:\n{result.stdout[-3000:]}"
    )
    assert "S1 final_answer:" in result.stdout, (
        f"S1 final_answer log line missing:\n{result.stdout[-3000:]}"
    )

    # S1 uses run_id="run-s1"; verify its orchestrator events.
    r = httpx.get(f"{orchestrator_url}/runs/run-s1", timeout=5.0)
    assert r.status_code == 200, r.text
    run = r.json()
    assert run["meta"]["graph_id"] == GRAPH_ID
    assert run["meta"]["trace_id"] == "trace-s1"

    events = run["events"]
    seen_events = [(e["event"], e.get("node_name", "")) for e in events]

    assert ("node-enter", "research") in seen_events
    assert ("node-exit", "research") in seen_events
    assert ("node-enter", "writer") in seen_events
    assert ("node-exit", "writer") in seen_events

    research_enter = next(i for i, ev in enumerate(seen_events) if ev == ("node-enter", "research"))
    writer_enter = next(i for i, ev in enumerate(seen_events) if ev == ("node-enter", "writer"))
    assert research_enter < writer_enter

    # Verify a Jaeger trace was emitted for the S1 run.
    deadline = time.time() + 30.0
    trace_found = False
    while time.time() < deadline and not trace_found:
        rj = httpx.get(
            f"{jaeger_url}/api/traces",
            params={"service": "main-langgraph", "lookback": "1h", "limit": 50},
            timeout=5.0,
        )
        if rj.status_code == 200:
            traces = rj.json().get("data", [])
            for trace in traces:
                spans = trace.get("spans", [])
                for span in spans:
                    tags = {t["key"]: t.get("value") for t in span.get("tags", [])}
                    if tags.get("amaze.run_id") == "run-s1":
                        trace_found = True
                        break
                if trace_found:
                    break
        if not trace_found:
            time.sleep(2.0)
    assert trace_found, "no Jaeger trace found with amaze.run_id=run-s1"


def test_st_rlg_5_missing_remote_node(temporarily_unregister_node, run_main_langgraph) -> None:
    temporarily_unregister_node(GRAPH_ID, "writer")
    result = run_main_langgraph(run_id="run-missing-1")
    assert result.returncode != 0, f"expected failure, got stdout={result.stdout}\nstderr={result.stderr}"
    output = result.stdout + result.stderr
    assert "RemoteNodeNotRegistered" in output or "node-not-registered" in output, output


def test_st_rlg_6_invalid_remote_response(research_with_env, run_main_langgraph) -> None:
    research_with_env({"AMAZE_DEBUG_BAD_PATCH": "1"})
    result = run_main_langgraph(run_id="run-bad-patch-1")
    assert result.returncode != 0, f"expected failure, got stdout={result.stdout}\nstderr={result.stderr}"
    output = result.stdout + result.stderr
    assert "InvalidStatePatch" in output or "invalid-state-patch" in output, output
