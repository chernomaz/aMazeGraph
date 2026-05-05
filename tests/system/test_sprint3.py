"""Sprint 3 system tests — ST-RLG-15 through ST-RLG-18.

All tests (except ST-RLG-16) rely on the `sprint3_demo` session fixture
(tests/conftest.py) which starts the S3 services and runs `main-langgraph`
once, caching the result.

Run against an already-up stack:

  AMAZEGRAPH_SKIP_COMPOSE=1 \\
  AMAZEGRAPH_COMPOSE_PROJECT=amazegraph-remote-langgraph \\
  ORCHESTRATOR_URL=http://localhost:8011 \\
  JAEGER_URL=http://localhost:16696 \\
  /home/ubuntu/venv/bin/python -m pytest tests/system/test_sprint3.py -v
"""

from __future__ import annotations

import datetime
import os
import subprocess
from typing import Any

import httpx
import pytest

GRAPH_ID = "demo_graph_v1"
ORCHESTRATOR_URL = os.environ.get("ORCHESTRATOR_URL", "http://localhost:8011")
JAEGER_URL = os.environ.get("JAEGER_URL", "http://localhost:16696")


# ── Helpers ───────────────────────────────────────────────────────────────────


def _get_run(orchestrator_url: str, run_id: str, *, timeout: float = 5.0) -> dict[str, Any]:
    r = httpx.get(f"{orchestrator_url}/runs/{run_id}", timeout=timeout)
    assert r.status_code == 200, (
        f"GET /runs/{run_id} returned {r.status_code}: {r.text[:512]}"
    )
    return r.json()


def _events_for(run: dict[str, Any], event_type: str, node_name: str | None = None) -> list[dict]:
    return [
        e
        for e in run["events"]
        if e.get("event") == event_type
        and (node_name is None or e.get("node_name") == node_name)
    ]


def _require_node(orchestrator_url: str, node_name: str) -> None:
    try:
        r = httpx.get(
            f"{orchestrator_url}/resolve/node/{GRAPH_ID}/{node_name}",
            timeout=5.0,
        )
    except httpx.TransportError as exc:
        pytest.skip(f"orchestrator unreachable ({exc}); is the stack running?")
    if r.status_code != 200:
        pytest.skip(f"node '{node_name}' not registered; is the service running?")


# ── ST-RLG-15: Subgraph node (cases #19 + #20) ───────────────────────────────


def test_st_rlg_15_subgraph_node(
    sprint3_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """Cases #19 + #20: remote 'subgraph' node internally runs a 2-step StateGraph.

    Verifies:
    1. Demo output shows step_a and step_b results in research_result.
    2. Orchestrator: node-enter and node-exit for 'subgraph' in run-s8.
    3. run-s8 ended with status=done.
    """
    _require_node(orchestrator_url, "subgraph")

    out = sprint3_demo.stdout + sprint3_demo.stderr

    # (1) Demo output must show step_a and step_b in S8 research_result
    assert "S8 research_result:" in out, (
        f"S8 research_result line missing from demo output:\n{out[-3000:]}"
    )
    s8_line = next((l for l in out.splitlines() if "S8 research_result:" in l), "")
    assert "step_a" in s8_line, f"step_a not found in S8 output: {s8_line}"
    assert "step_b" in s8_line, f"step_b not found in S8 output: {s8_line}"

    # (2) Orchestrator: subgraph node entered and exited
    run = _get_run(orchestrator_url, "run-s8")
    enters = _events_for(run, "node-enter", "subgraph")
    exits = _events_for(run, "node-exit", "subgraph")
    assert enters, "no node-enter for 'subgraph' in run-s8"
    assert exits, "no node-exit for 'subgraph' in run-s8"

    # (3) run-s8 completed successfully
    run_end = _events_for(run, "run-end")
    assert run_end and run_end[-1].get("status") == "done", (
        f"run-s8 did not end with status=done: {run_end}"
    )


# ── ST-RLG-16: Error taxonomy (case #27) ─────────────────────────────────────


def test_st_rlg_16_error_kind_orchestrator_roundtrip(
    orchestrator_url: str,
) -> None:
    """Case #27: error_kind field is stored and returned by the orchestrator.

    Directly posts a node-error event with error_kind and verifies it
    round-trips via GET /runs/{run_id}. Tests the orchestrator layer; the
    proxy layer is verified by the error_kind values emitted in run-s5/s6.
    """
    run_id = "run-st16-taxonomy"
    for kind, desc in [
        ("node_error", "invalid-state-patch"),
        ("proxy_block", "node-not-registered"),
        ("timeout", "timeout: read timeout"),
    ]:
        r = httpx.post(
            f"{orchestrator_url}/runs/{run_id}/events",
            json={
                "event": "node-error",
                "graph_id": GRAPH_ID,
                "node_name": "research",
                "trace_id": "trace-st16",
                "status": "error",
                "error": desc,
                "error_kind": kind,
                "ts": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            },
            timeout=5.0,
        )
        assert r.status_code == 200, f"POST /runs event failed: {r.text}"

    run = _get_run(orchestrator_url, run_id)
    error_events = _events_for(run, "node-error")
    assert len(error_events) >= 3, f"expected 3 error events, got {len(error_events)}"

    returned_kinds = {e.get("error_kind") for e in error_events}
    for kind in ("node_error", "proxy_block", "timeout"):
        assert kind in returned_kinds, (
            f"error_kind='{kind}' not found in returned events: {returned_kinds}"
        )


def test_st_rlg_16_error_kind_proxy_node_error(
    sprint3_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
    research_with_env,
    run_main_langgraph,
) -> None:
    """Case #27: proxy emits error_kind=node_error when research returns bad data.

    Uses AMAZE_DEBUG_BAD_PATCH=1 to make the research node return a non-dict
    state_patch, then verifies the orchestrator stream has error_kind=node_error.
    """
    _require_node(orchestrator_url, "research")

    # Start a debug research container that returns bad data
    research_with_env({"AMAZE_DEBUG_BAD_PATCH": "1"})

    # Run the demo (S1 will fail because research returns bad patch)
    run_main_langgraph()

    # Check run-s1 events for error_kind=node_error
    # (run-s1 is reused; new events are appended to the same stream)
    run = _get_run(orchestrator_url, "run-s1")
    error_events = _events_for(run, "node-error", "research")
    assert error_events, "no node-error for 'research' in run-s1 after bad-patch run"

    # The last error event (from the bad-patch run) must have error_kind=node_error
    last_err = error_events[-1]
    assert last_err.get("error_kind") == "node_error", (
        f"expected error_kind=node_error, got: {last_err}"
    )


# ── ST-RLG-17: Recursion / step metadata (case #25) ──────────────────────────


def test_st_rlg_17_recursion_step_metadata(
    sprint3_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """Case #25: langgraph_step survives the wire; counter loops 3 times.

    Verifies:
    1. Demo output shows final count >= 3.
    2. Demo output shows langgraph_step_echo is not None (step metadata propagated).
    3. Orchestrator: multiple node-enter events for 'counter' in run-s10.
    4. run-s10 ended with status=done.
    """
    _require_node(orchestrator_url, "counter")

    out = sprint3_demo.stdout + sprint3_demo.stderr

    # (1) & (2) Demo log line
    assert "S10 count=" in out, (
        f"S10 count= line missing from demo output:\n{out[-3000:]}"
    )
    s10_line = next((l for l in out.splitlines() if "S10 count=" in l), "")
    # count should be >= 3
    import re
    m = re.search(r"count=(\d+)", s10_line)
    assert m, f"could not parse count from S10 line: {s10_line}"
    assert int(m.group(1)) >= 3, f"expected count >= 3, got: {s10_line}"

    # langgraph_step_echo must be non-None (value > 0 on last iteration)
    step_m = re.search(r"langgraph_step_echo=(\d+)", s10_line)
    assert step_m, f"langgraph_step_echo missing from S10 line: {s10_line}"

    # (3) Multiple counter invocations in run-s10
    run = _get_run(orchestrator_url, "run-s10")
    enters = _events_for(run, "node-enter", "counter")
    assert len(enters) >= 3, (
        f"expected at least 3 node-enter for 'counter', got {len(enters)}"
    )

    # (4) run-s10 completed successfully
    run_end = _events_for(run, "run-end")
    assert run_end and run_end[-1].get("status") == "done", (
        f"run-s10 did not end with status=done: {run_end}"
    )


# ── ST-RLG-18: Input / output / private schemas (case #28) ───────────────────


def test_st_rlg_18_schema_split(
    sprint3_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """Case #28: ainvoke() returns only OutputState fields; private fields absent.

    Verifies:
    1. Demo output shows final_answer present in S9 result.
    2. Demo output shows private_data absent from S9 result keys.
    3. Orchestrator: node-enter and node-exit for 'schema_remote' in run-s9.
    4. run-s9 ended with status=done.
    """
    _require_node(orchestrator_url, "schema_remote")

    out = sprint3_demo.stdout + sprint3_demo.stderr

    # (1) final_answer must be present
    assert "S9 final_answer:" in out, (
        f"S9 final_answer line missing from demo output:\n{out[-3000:]}"
    )
    fa_line = next((l for l in out.splitlines() if "S9 final_answer:" in l), "")
    assert fa_line.strip() != "S9 final_answer:", "S9 final_answer is empty"

    # (2) private_data must NOT appear in the result keys
    keys_line = next((l for l in out.splitlines() if "S9 keys in result:" in l), "")
    assert keys_line, f"S9 keys line missing:\n{out[-2000:]}"
    assert "private_data" not in keys_line, (
        f"private_data leaked into output: {keys_line}"
    )
    assert "final_answer" in keys_line, (
        f"final_answer missing from output keys: {keys_line}"
    )

    # (3) schema_remote node entered and exited
    run = _get_run(orchestrator_url, "run-s9")
    enters = _events_for(run, "node-enter", "schema_remote")
    exits = _events_for(run, "node-exit", "schema_remote")
    assert enters, "no node-enter for 'schema_remote' in run-s9"
    assert exits, "no node-exit for 'schema_remote' in run-s9"

    # (4) run-s9 completed successfully
    run_end = _events_for(run, "run-end")
    assert run_end and run_end[-1].get("status") == "done", (
        f"run-s9 did not end with status=done: {run_end}"
    )
