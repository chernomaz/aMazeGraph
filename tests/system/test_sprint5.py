"""Sprint 5 system tests — ST-RLG-23 through ST-RLG-25.

All tests rely on the `sprint5_demo` session fixture (tests/conftest.py) which
starts the remote-send service and runs `main-langgraph` once, caching the result.

Run against an already-up stack:

  AMAZEGRAPH_SKIP_COMPOSE=1 \\
  AMAZEGRAPH_COMPOSE_PROJECT=amazegraph-remote-langgraph \\
  ORCHESTRATOR_URL=http://localhost:8011 \\
  JAEGER_URL=http://localhost:16696 \\
  /home/ubuntu/venv/bin/python -m pytest tests/system/test_sprint5.py -v
"""

from __future__ import annotations

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
        pytest.skip(f"node '{node_name}' not registered; is remote-send running?")


# ── Guard: demo must exit 0 ──────────────────────────────────────────────────


def test_sprint5_demo_exit_code(sprint5_demo: subprocess.CompletedProcess) -> None:
    """Fail fast if the Sprint 5 demo crashed — all scenarios passed or were SKIPped."""
    assert sprint5_demo.returncode == 0, (
        f"Sprint 5 demo exited {sprint5_demo.returncode}.\n"
        f"stdout:\n{sprint5_demo.stdout[-3000:]}\n"
        f"stderr:\n{sprint5_demo.stderr[-3000:]}"
    )


# ── ST-RLG-23: Single Send (case #13) ────────────────────────────────────────


def test_st_rlg_23_send_single(
    sprint5_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """Case #13: remote node returns Command(goto=[Send('send_sink', {'val': ...})]).

    The sink node receives only the Send payload — not the full graph state.

    Verifies:
    1. Demo output shows S14 send_received contains 'val' key.
    2. send_received does NOT contain 'full_state_marker' (proving custom payload
       arrived, not the merged graph state).
    3. Orchestrator run-s14: send_dispatcher node-exit with status=ok.
    4. run-s14 ended with status=done.
    """
    _require_node(orchestrator_url, "send_dispatcher")

    out = sprint5_demo.stdout + sprint5_demo.stderr

    # (1+2) Demo output — send_received has 'val', lacks 'full_state_marker'
    assert "S14 send_received:" in out, f"S14 send_received line missing:\n{out[-3000:]}"
    s14_line = next((l for l in out.splitlines() if "S14 send_received:" in l), "")
    assert "'val'" in s14_line, (
        f"'val' key not found in S14 send_received: {s14_line}"
    )
    assert "full_state_marker" not in s14_line, (
        f"'full_state_marker' leaked into Send payload: {s14_line}"
    )

    # (3) send_dispatcher node-exit ok
    run = _get_run(orchestrator_url, "run-s14")
    dispatcher_exits = _events_for(run, "node-exit", "send_dispatcher")
    assert dispatcher_exits, "no node-exit for 'send_dispatcher' in run-s14"
    assert dispatcher_exits[-1].get("status") == "ok", (
        f"send_dispatcher node-exit status != ok: {dispatcher_exits[-1]}"
    )

    # (4) run-s14 done
    run_ends = _events_for(run, "run-end")
    assert run_ends and run_ends[-1].get("status") == "done", (
        f"run-s14 did not end with status=done: {run_ends}"
    )

    s14_lines = [l for l in out.splitlines() if "S14" in l]
    print("\n── S14 output ──")
    for line in s14_lines:
        print(line)
    print(f"── orchestrator events for run-s14: {[e['event'] + '/' + (e.get('node_name') or '-') for e in run['events']]} ──")


# ── ST-RLG-24: Parallel Send fan-out (case #13) ──────────────────────────────


def test_st_rlg_24_send_parallel_fanout(
    sprint5_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """Case #13: Command(goto=[Send('send_sink_a', ...), Send('send_sink_b', ...)]).

    Both branches execute concurrently; each receives its own distinct payload.

    Verifies:
    1. Demo output shows S15 send_results contains both branch_a and branch_b entries.
    2. Orchestrator run-s15: send_dispatcher node-exit ok.
    3. Orchestrator run-s15: node-enter events for both 'send_sink_a' and 'send_sink_b'
       are absent (they are local nodes) — instead, verify via demo output.
    4. run-s15 ended with status=done.
    """
    _require_node(orchestrator_url, "send_dispatcher")

    out = sprint5_demo.stdout + sprint5_demo.stderr

    # (1) Both branches produced results
    assert "S15 send_results:" in out, f"S15 send_results line missing:\n{out[-3000:]}"
    s15_line = next((l for l in out.splitlines() if "S15 send_results:" in l), "")
    assert "branch_a" in s15_line, (
        f"branch_a result missing from S15 send_results: {s15_line}"
    )
    assert "branch_b" in s15_line, (
        f"branch_b result missing from S15 send_results: {s15_line}"
    )

    # (2) send_dispatcher node-exit ok
    run = _get_run(orchestrator_url, "run-s15")
    dispatcher_exits = _events_for(run, "node-exit", "send_dispatcher")
    assert dispatcher_exits, "no node-exit for 'send_dispatcher' in run-s15"
    assert dispatcher_exits[-1].get("status") == "ok", (
        f"send_dispatcher node-exit status != ok: {dispatcher_exits[-1]}"
    )

    # (3) run-s15 done
    run_ends = _events_for(run, "run-end")
    assert run_ends and run_ends[-1].get("status") == "done", (
        f"run-s15 did not end with status=done: {run_ends}"
    )

    s15_lines = [l for l in out.splitlines() if "S15" in l]
    print("\n── S15 output ──")
    for line in s15_lines:
        print(line)
    print(f"── orchestrator events for run-s15: {[e['event'] + '/' + (e.get('node_name') or '-') for e in run['events']]} ──")


# ── ST-RLG-25: Command + Send (case #16) ─────────────────────────────────────


def test_st_rlg_25_command_send_with_update(
    sprint5_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """Case #16: Command(update={'status':'dispatched'}, goto=[Send('send_sink', ...)]).

    State update and Send routing happen in the same response.

    Verifies:
    1. Demo output shows S16 status == 'dispatched' (from Command.update).
    2. Demo output shows S16 send_received contains 'val' (from Send payload).
    3. Orchestrator run-s16: send_dispatcher node-exit ok.
    4. run-s16 ended with status=done.
    """
    _require_node(orchestrator_url, "send_dispatcher")

    out = sprint5_demo.stdout + sprint5_demo.stderr

    # (1) Command.update applied: status field set
    assert "S16 status:" in out, f"S16 status line missing:\n{out[-3000:]}"
    s16_line = next((l for l in out.splitlines() if "S16 status:" in l), "")
    assert "dispatched" in s16_line, (
        f"expected 'dispatched' in S16 status: {s16_line}"
    )

    # (2) Send payload delivered: send_received has 'val'
    assert "S16" in out and "send_received" in out, (
        f"S16 send_received line missing:\n{out[-3000:]}"
    )
    s16_recv_line = next(
        (l for l in out.splitlines() if "S16" in l and "send_received" in l), ""
    )
    assert "val" in s16_recv_line, (
        f"'val' key not found in S16 send_received: {s16_recv_line}"
    )

    # (3) send_dispatcher node-exit ok
    run = _get_run(orchestrator_url, "run-s16")
    dispatcher_exits = _events_for(run, "node-exit", "send_dispatcher")
    assert dispatcher_exits, "no node-exit for 'send_dispatcher' in run-s16"
    assert dispatcher_exits[-1].get("status") == "ok", (
        f"send_dispatcher node-exit status != ok: {dispatcher_exits[-1]}"
    )

    # (4) run-s16 done
    run_ends = _events_for(run, "run-end")
    assert run_ends and run_ends[-1].get("status") == "done", (
        f"run-s16 did not end with status=done: {run_ends}"
    )

    s16_lines = [l for l in out.splitlines() if "S16" in l]
    print("\n── S16 output ──")
    for line in s16_lines:
        print(line)
    print(f"── orchestrator events for run-s16: {[e['event'] + '/' + (e.get('node_name') or '-') for e in run['events']]} ──")


# ── ST-RLG-26: bare Send, no Command wrapper ─────────────────────────────────


def test_st_rlg_26_bare_send(
    sprint5_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """Remote node returns Send(...) directly with no Command wrapper.

    The proxy normalises it to Command(goto=[Send(...)]) on the wire;
    the driver and LangGraph are unaware of the difference.

    Verifies:
    1. Demo output shows S17 send_received contains 'val'.
    2. send_received does NOT contain 'full_state_marker' (custom arg, not full state).
    3. Orchestrator run-s17: send_dispatcher node-exit ok.
    4. run-s17 ended with status=done.
    """
    _require_node(orchestrator_url, "send_dispatcher")

    out = sprint5_demo.stdout + sprint5_demo.stderr

    # (1+2) send_received has 'val', lacks 'full_state_marker'
    assert "S17 send_received:" in out, f"S17 send_received line missing:\n{out[-3000:]}"
    s17_line = next((l for l in out.splitlines() if "S17 send_received:" in l), "")
    assert "'val'" in s17_line, (
        f"'val' key not found in S17 send_received: {s17_line}"
    )
    assert "full_state_marker" not in s17_line, (
        f"'full_state_marker' leaked into bare Send payload: {s17_line}"
    )

    # (3) send_dispatcher node-exit ok
    run = _get_run(orchestrator_url, "run-s17")
    dispatcher_exits = _events_for(run, "node-exit", "send_dispatcher")
    assert dispatcher_exits, "no node-exit for 'send_dispatcher' in run-s17"
    assert dispatcher_exits[-1].get("status") == "ok", (
        f"send_dispatcher node-exit status != ok: {dispatcher_exits[-1]}"
    )

    # (4) run-s17 done
    run_ends = _events_for(run, "run-end")
    assert run_ends and run_ends[-1].get("status") == "done", (
        f"run-s17 did not end with status=done: {run_ends}"
    )

    s17_lines = [l for l in out.splitlines() if "S17" in l]
    print("\n── S17 output ──")
    for line in s17_lines:
        print(line)
    print(f"── orchestrator events for run-s17: {[e['event'] + '/' + (e.get('node_name') or '-') for e in run['events']]} ──")
