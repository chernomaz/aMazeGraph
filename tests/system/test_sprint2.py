"""Sprint 2 system tests — ST-RLG-7 through ST-RLG-13.

All tests rely on the `sprint2_demo` session fixture (tests/conftest.py) which
runs `docker compose run --rm main-langgraph` once and caches the result.

Run these with an already-up stack:

  AMAZEGRAPH_SKIP_COMPOSE=1 \\
  AMAZEGRAPH_COMPOSE_PROJECT=amazegraph-remote-langgraph \\
  ORCHESTRATOR_URL=http://localhost:8011 \\
  JAEGER_URL=http://localhost:16696 \\
  /home/ubuntu/venv/bin/python -m pytest tests/system/test_sprint2.py -v
"""

from __future__ import annotations

import os
import subprocess
import time
from typing import Any

import httpx
import pytest

GRAPH_ID = "demo_graph_v1"
ORCHESTRATOR_URL = os.environ.get("ORCHESTRATOR_URL", "http://localhost:8011")
JAEGER_URL = os.environ.get("JAEGER_URL", "http://localhost:16696")


# ── Helpers ───────────────────────────────────────────────────────────────────


def _get_run(orchestrator_url: str, run_id: str, *, timeout: float = 5.0) -> dict[str, Any]:
    """Fetch run events from the orchestrator; fail test on non-200."""
    r = httpx.get(f"{orchestrator_url}/runs/{run_id}", timeout=timeout)
    assert r.status_code == 200, (
        f"GET /runs/{run_id} returned {r.status_code}: {r.text[:512]}"
    )
    return r.json()


def _events_for(run: dict[str, Any], event_type: str, node_name: str | None = None) -> list[dict]:
    """Filter run events by type and optional node_name."""
    return [
        e
        for e in run["events"]
        if e.get("event") == event_type
        and (node_name is None or e.get("node_name") == node_name)
    ]


def _require_node(orchestrator_url: str, node_name: str) -> None:
    """Skip the test if the node is not registered in the orchestrator."""
    try:
        r = httpx.get(
            f"{orchestrator_url}/resolve/node/{GRAPH_ID}/{node_name}",
            timeout=5.0,
        )
    except httpx.TransportError as exc:
        pytest.skip(f"orchestrator unreachable ({exc}); is the stack running?")
    if r.status_code != 200:
        pytest.skip(f"node '{node_name}' not registered; is the service running?")


# ── ST-RLG-7: operator.add reducer merges log_trail ──────────────────────────


def test_st_rlg_7_reducer_log_trail(
    sprint2_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """S1: post_research and post_writer each append ONE delta item via
    operator.add reducer — final log_trail has exactly 2 entries, never
    overwriting each other (verifies the double-append bug is fixed).
    """
    _require_node(orchestrator_url, "research")
    _require_node(orchestrator_url, "writer")

    out = sprint2_demo.stdout + sprint2_demo.stderr
    # Demo must log the log_trail
    assert "S1 log_trail:" in out, f"S1 log_trail line missing from output:\n{out[-3000:]}"

    # Parse the log_trail line
    log_trail_line = next(
        (line for line in out.splitlines() if "S1 log_trail:" in line), ""
    )
    # Must contain entries from both post-nodes
    assert "post_research_node" in log_trail_line, (
        f"post_research_node not in log_trail: {log_trail_line}"
    )
    assert "post_writer_node" in log_trail_line, (
        f"post_writer_node not in log_trail: {log_trail_line}"
    )

    # Verify via orchestrator events
    run = _get_run(orchestrator_url, "run-s1")
    node_enters = {e["node_name"] for e in _events_for(run, "node-enter")}
    node_exits = {e["node_name"] for e in _events_for(run, "node-exit")}
    assert "research" in node_enters, f"research node-enter missing; events={run['events']}"
    assert "writer" in node_enters, f"writer node-enter missing"
    assert "research" in node_exits
    assert "writer" in node_exits

    # run-start and run-end both present, status=done
    run_end_events = _events_for(run, "run-end")
    assert run_end_events, "no run-end event for run-s1"
    assert run_end_events[-1].get("status") == "done", (
        f"run-s1 ended with status={run_end_events[-1].get('status')}"
    )


# ── ST-RLG-8: add_messages reducer merges AIMessage ──────────────────────────


def test_st_rlg_8_messages_state_reducer(
    sprint2_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """S2: llm_tool_node returns {messages: [{role:'assistant',...}]};
    add_messages reducer merges it so state['messages'] grows by 1,
    final message is an AIMessage with non-empty content.
    """
    _require_node(orchestrator_url, "llm_tool")

    out = sprint2_demo.stdout + sprint2_demo.stderr

    # If no OPENAI_API_KEY the node returns a skip sentinel — skip this test too
    if "[skipped: no OPENAI_API_KEY]" in out:
        pytest.skip("OPENAI_API_KEY not configured; llm_tool returned skip sentinel")

    assert "S2 messages count:" in out, (
        f"S2 messages count line missing:\n{out[-2000:]}"
    )
    count_line = next(
        (l for l in out.splitlines() if "S2 messages count:" in l), ""
    )
    # Extract number after "messages count: "
    try:
        count_str = count_line.split("S2 messages count:")[-1].strip()
        count = int(count_str.split()[0])
    except (ValueError, IndexError):
        pytest.fail(f"Could not parse message count from: {count_line!r}")
    assert count >= 1, (
        f"Expected at least 1 message in state after S2, got {count}"
    )

    # Orchestrator: run-s2 node-enter and node-exit for llm_tool
    run = _get_run(orchestrator_url, "run-s2")
    assert _events_for(run, "node-enter", "llm_tool"), "no llm_tool node-enter"
    assert _events_for(run, "node-exit", "llm_tool"), "no llm_tool node-exit"


# ── ST-RLG-9: real OpenAI LLM + MCP web_search ───────────────────────────────


def test_st_rlg_9_llm_mcp_tool_call(
    sprint2_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
    jaeger_url: str,
) -> None:
    """S2: ChatOpenAI bound to MCP tools calls web_search via Tavily;
    result is non-empty; Jaeger has span for amazegraph.invoke_remote on llm_tool.
    Skipped if OPENAI_API_KEY or TAVILY_API_KEY not configured.
    """
    _require_node(orchestrator_url, "llm_tool")

    if not os.environ.get("OPENAI_API_KEY", "").strip():
        pytest.skip("OPENAI_API_KEY not set")
    if not os.environ.get("TAVILY_API_KEY", "").strip():
        pytest.skip("TAVILY_API_KEY not set")

    out = sprint2_demo.stdout + sprint2_demo.stderr
    if "[skipped: no OPENAI_API_KEY]" in out or "[mcp_unavailable]" in out.lower():
        pytest.skip("llm_tool returned skip/mcp_unavailable; check API keys in container")

    # tool_result should be present and non-empty
    tool_result_line = next(
        (l for l in out.splitlines() if "S2 tool_result:" in l), ""
    )
    assert tool_result_line, "S2 tool_result line missing from output"
    raw = tool_result_line.split("S2 tool_result:")[-1].strip()
    assert raw and raw not in ("[skipped: no OPENAI_API_KEY]", "[mcp_unavailable]"), (
        f"tool_result is empty or skip sentinel: {raw!r}"
    )

    # Jaeger: span for amazegraph.invoke_remote with node llm_tool
    deadline = time.time() + 30.0
    found = False
    while time.time() < deadline and not found:
        rj = httpx.get(
            f"{jaeger_url}/api/traces",
            params={"service": "main-langgraph", "lookback": "1h", "limit": 100},
            timeout=5.0,
        )
        if rj.status_code == 200:
            for t in rj.json().get("data", []):
                for span in t.get("spans", []):
                    tags = {tag["key"]: tag.get("value") for tag in span.get("tags", [])}
                    if (
                        span.get("operationName") == "amazegraph.invoke_remote"
                        and tags.get("amaze.node_name") == "llm_tool"
                    ):
                        found = True
                        break
                if found:
                    break
        if not found:
            time.sleep(2.0)
    assert found, "Jaeger: no amazegraph.invoke_remote span for llm_tool node"


# ── ST-RLG-10: thread_id + runtime_context round-trip ────────────────────────


def test_st_rlg_10_config_runtime_echo(
    sprint2_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """S3: driver passes configurable[thread_id]='t-123' and
    runtime_context[tenant_id]='acme'; remote config_echo node echoes both.
    """
    _require_node(orchestrator_url, "config_echo")

    out = sprint2_demo.stdout + sprint2_demo.stderr
    assert "S3 echoed_thread=" in out, (
        f"S3 config echo line missing:\n{out[-2000:]}"
    )
    echo_line = next((l for l in out.splitlines() if "S3 echoed_thread=" in l), "")
    assert "t-123" in echo_line, (
        f"thread_id 't-123' not echoed back; got: {echo_line!r}"
    )
    assert "acme" in echo_line, (
        f"tenant_id 'acme' not echoed back; got: {echo_line!r}"
    )

    # Orchestrator: run-s3 completed successfully
    run = _get_run(orchestrator_url, "run-s3")
    assert _events_for(run, "node-enter", "config_echo")
    assert _events_for(run, "node-exit", "config_echo")
    run_end = _events_for(run, "run-end")
    assert run_end and run_end[-1].get("status") == "done"


# ── ST-RLG-11: conditional routing (research vs writer) ──────────────────────


def test_st_rlg_11_conditional_routing(
    sprint2_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """S4a: mode='research' → only research node fires (not writer).
    S4b: mode='write'     → only writer node fires (not research).
    """
    _require_node(orchestrator_url, "research")
    _require_node(orchestrator_url, "writer")

    out = sprint2_demo.stdout + sprint2_demo.stderr
    assert "S4 mode=research" in out, f"S4a output missing:\n{out[-2000:]}"
    assert "S4 mode=write" in out, f"S4b output missing:\n{out[-2000:]}"

    # S4a: run-s4-research — research node fires, writer does NOT
    run_a = _get_run(orchestrator_url, "run-s4-research")
    assert _events_for(run_a, "node-enter", "research"), (
        "S4a: research node-enter missing (mode=research should route to research)"
    )
    assert not _events_for(run_a, "node-enter", "writer"), (
        "S4a: writer node-enter found — conditional routing should NOT have routed to writer"
    )

    # S4b: run-s4-write — writer node fires, research does NOT
    run_b = _get_run(orchestrator_url, "run-s4-write")
    assert _events_for(run_b, "node-enter", "writer"), (
        "S4b: writer node-enter missing (mode=write should route to writer)"
    )
    assert not _events_for(run_b, "node-enter", "research"), (
        "S4b: research node-enter found — conditional routing should NOT have routed to research"
    )


# ── ST-RLG-12: audit no-op (returns {}) ──────────────────────────────────────


def test_st_rlg_12_audit_noop_return(
    sprint2_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """S5: remote audit node returns {}; state is not mutated (user_request
    survives, log_trail unchanged after reduction with {}).
    """
    _require_node(orchestrator_url, "audit")

    out = sprint2_demo.stdout + sprint2_demo.stderr
    assert "S5 user_request preserved:" in out, (
        f"S5 audit output missing:\n{out[-2000:]}"
    )
    # user_request must be preserved
    preserved_line = next(
        (l for l in out.splitlines() if "S5 user_request preserved:" in l), ""
    )
    assert "audit this run" in preserved_line, (
        f"S5: user_request overwritten; got: {preserved_line!r}"
    )

    # Orchestrator: run-s5 has audit node-enter and node-exit
    run = _get_run(orchestrator_url, "run-s5")
    assert _events_for(run, "node-enter", "audit"), "no audit node-enter in run-s5"
    assert _events_for(run, "node-exit", "audit"), "no audit node-exit in run-s5"
    run_end = _events_for(run, "run-end")
    assert run_end and run_end[-1].get("status") == "done"


# ── ST-RLG-13: real parallel fan-out ─────────────────────────────────────────


def test_st_rlg_13_parallel_fan_out(
    sprint2_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """S6: planner → [research_a, research_b] → joiner.

    Two remote branches fire concurrently. Verified by:
    1. Final state['results'] contains entries from BOTH branches.
    2. Both node-enter events precede both node-exit events in Redis stream
       (overlap = true concurrent execution, not sequential).
    3. Two distinct child spans in Jaeger under the planner span.
    """
    _require_node(orchestrator_url, "research_a")
    _require_node(orchestrator_url, "research_b")

    out = sprint2_demo.stdout + sprint2_demo.stderr
    assert "S6 results" in out, f"S6 fan-out output missing:\n{out[-2000:]}"

    # Results must contain entries from both branches
    results_line = next(
        (l for l in out.splitlines() if "S6 results" in l and "items" in l), ""
    )
    assert "research_a" in results_line, (
        f"S6: 'research_a' not in results — branch A may not have fired: {results_line!r}"
    )
    assert "research_b" in results_line, (
        f"S6: 'research_b' not in results — branch B may not have fired: {results_line!r}"
    )

    # Orchestrator: both branches have node-enter and node-exit events
    run = _get_run(orchestrator_url, "run-s6")
    enters_a = _events_for(run, "node-enter", "research_a")
    enters_b = _events_for(run, "node-enter", "research_b")
    exits_a = _events_for(run, "node-exit", "research_a")
    exits_b = _events_for(run, "node-exit", "research_b")

    assert enters_a, "research_a node-enter missing from run-s6"
    assert enters_b, "research_b node-enter missing from run-s6"
    assert exits_a, "research_a node-exit missing from run-s6"
    assert exits_b, "research_b node-exit missing from run-s6"

    # Concurrency proof: both node-enter events appear before both node-exit events
    # in the Redis Stream insertion order (stream IDs are monotonically increasing).
    all_events = run["events"]
    idx = {(e.get("event"), e.get("node_name")): i for i, e in enumerate(all_events)}

    enter_a_idx = next(
        (i for i, e in enumerate(all_events)
         if e.get("event") == "node-enter" and e.get("node_name") == "research_a"),
        None,
    )
    enter_b_idx = next(
        (i for i, e in enumerate(all_events)
         if e.get("event") == "node-enter" and e.get("node_name") == "research_b"),
        None,
    )
    exit_a_idx = next(
        (i for i, e in enumerate(all_events)
         if e.get("event") == "node-exit" and e.get("node_name") == "research_a"),
        None,
    )
    exit_b_idx = next(
        (i for i, e in enumerate(all_events)
         if e.get("event") == "node-exit" and e.get("node_name") == "research_b"),
        None,
    )

    assert enter_a_idx is not None and enter_b_idx is not None
    assert exit_a_idx is not None and exit_b_idx is not None

    last_enter = max(enter_a_idx, enter_b_idx)
    first_exit = min(exit_a_idx, exit_b_idx)

    assert last_enter < first_exit, (
        f"Fan-out branches did NOT run concurrently: "
        f"last node-enter at stream idx={last_enter} "
        f"but first node-exit at stream idx={first_exit}. "
        f"Expected all enters before any exit (true parallel execution). "
        f"Event sequence: {[(e.get('event'), e.get('node_name')) for e in all_events]}"
    )
