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
    result = run_main_langgraph(run_id="run-manifest-1")
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"

    r = httpx.get(f"{orchestrator_url}/graphs/{GRAPH_ID}", timeout=5.0)
    assert r.status_code == 200, r.text
    manifest = r.json()
    assert manifest["graph_id"] == GRAPH_ID
    assert set(manifest["nodes"]) >= {"start", "research", "post_research", "writer", "post_writer"}
    edge_pairs = {tuple(e) for e in manifest["edges"]}
    assert ("start", "research") in edge_pairs
    assert ("research", "post_research") in edge_pairs
    assert ("post_research", "writer") in edge_pairs
    assert ("writer", "post_writer") in edge_pairs


def test_st_rlg_4_end_to_end_run(orchestrator_url: str, jaeger_url: str, run_main_langgraph) -> None:
    result = run_main_langgraph()
    assert result.returncode == 0, f"stdout={result.stdout}\nstderr={result.stderr}"
    assert "FINAL RESULT:" in result.stdout
    assert "final_answer" in result.stdout.lower() or "FINAL RESULT:" in result.stdout

    r = httpx.get(f"{orchestrator_url}/runs/run-1", timeout=5.0)
    assert r.status_code == 200, r.text
    run = r.json()
    assert run["meta"]["graph_id"] == GRAPH_ID
    assert run["meta"]["trace_id"] == "trace-1"

    events = run["events"]
    seen_events = [(e["event"], e.get("node_name", "")) for e in events]

    assert ("node-enter", "research") in seen_events
    assert ("node-exit", "research") in seen_events
    assert ("node-enter", "writer") in seen_events
    assert ("node-exit", "writer") in seen_events

    research_enter = next(i for i, ev in enumerate(seen_events) if ev == ("node-enter", "research"))
    writer_enter = next(i for i, ev in enumerate(seen_events) if ev == ("node-enter", "writer"))
    assert research_enter < writer_enter

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
                    if tags.get("amaze.run_id") == "run-1":
                        trace_found = True
                        break
                if trace_found:
                    break
        if not trace_found:
            time.sleep(2.0)
    assert trace_found, "no Jaeger trace found with amaze.run_id=run-1"


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
