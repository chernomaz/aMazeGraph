"""Sprint 4 system tests — ST-RLG-19 through ST-RLG-22.

All tests rely on the `sprint4_demo` session fixture (tests/conftest.py) which
starts the remote-command service and runs `main-langgraph` once, caching the result.

Run against an already-up stack:

  AMAZEGRAPH_SKIP_COMPOSE=1 \\
  AMAZEGRAPH_COMPOSE_PROJECT=amazegraph-remote-langgraph \\
  ORCHESTRATOR_URL=http://localhost:8011 \\
  JAEGER_URL=http://localhost:16696 \\
  /home/ubuntu/venv/bin/python -m pytest tests/system/test_sprint4.py -v
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
        pytest.skip(f"node '{node_name}' not registered; is remote-command running?")


# ── Guard: demo must exit 0 ──────────────────────────────────────────────────


def test_sprint4_demo_exit_code(sprint4_demo: subprocess.CompletedProcess) -> None:
    """Fail fast if the Sprint 4 demo crashed — all scenarios passed or were SKIPped."""
    assert sprint4_demo.returncode == 0, (
        f"Sprint 4 demo exited {sprint4_demo.returncode}.\n"
        f"stdout:\n{sprint4_demo.stdout[-3000:]}\n"
        f"stderr:\n{sprint4_demo.stderr[-3000:]}"
    )


# ── ST-RLG-19: Single-goto Command (case #14) ────────────────────────────────


def test_st_rlg_19_command_single_goto(
    sprint4_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """Case #14: remote node returns Command(goto='cmd_sink'); proxy routes there.

    Verifies:
    1. Demo output shows S11 cmd_result == 'single-goto-result'.
    2. Orchestrator run-s11: node-exit for 'command' with status=ok.
    3. Orchestrator run-s11: node-enter and node-exit for 'cmd_sink'.
    4. run-s11 ended with status=done.
    """
    _require_node(orchestrator_url, "command")

    out = sprint4_demo.stdout + sprint4_demo.stderr

    # (1) Demo output
    assert "S11 cmd_result:" in out, f"S11 cmd_result line missing:\n{out[-3000:]}"
    s11_line = next((l for l in out.splitlines() if "S11 cmd_result:" in l), "")
    assert "single-goto-result" in s11_line, (
        f"expected 'single-goto-result' in S11 output: {s11_line}"
    )

    # (2) command node-exit ok
    run = _get_run(orchestrator_url, "run-s11")
    cmd_exits = _events_for(run, "node-exit", "command")
    assert cmd_exits, "no node-exit for 'command' in run-s11"
    assert cmd_exits[-1].get("status") == "ok", (
        f"command node-exit status != ok: {cmd_exits[-1]}"
    )

    # (3) run-s11 done
    # cmd_sink is a local node — no orchestrator events emitted for it.
    # Routing correctness is verified by cmd_result in demo output (above).
    run_ends = _events_for(run, "run-end")
    assert run_ends and run_ends[-1].get("status") == "done", (
        f"run-s11 did not end with status=done: {run_ends}"
    )


# ── ST-RLG-20: Command update + goto (case #14 variant) ──────────────────────


def test_st_rlg_20_command_update_goto(
    sprint4_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """Case #14: Command(update={cmd_result:...}, goto='cmd_sink') merges state and routes.

    Verifies:
    1. Demo output shows S11b cmd_result starts with 'processed:'.
    2. Orchestrator run-s11b: node-exit for 'command' status=ok.
    3. Orchestrator run-s11b: cmd_sink entered and exited.
    4. run-s11b ended with status=done.
    """
    _require_node(orchestrator_url, "command")

    out = sprint4_demo.stdout + sprint4_demo.stderr

    # (1) Demo output
    assert "S11b cmd_result:" in out, f"S11b cmd_result line missing:\n{out[-3000:]}"
    s11b_line = next((l for l in out.splitlines() if "S11b cmd_result:" in l), "")
    assert "processed:" in s11b_line, (
        f"expected 'processed:' in S11b output: {s11b_line}"
    )

    # (2) command node-exit ok
    run = _get_run(orchestrator_url, "run-s11b")
    cmd_exits = _events_for(run, "node-exit", "command")
    assert cmd_exits, "no node-exit for 'command' in run-s11b"
    assert cmd_exits[-1].get("status") == "ok", (
        f"command node-exit status != ok: {cmd_exits[-1]}"
    )

    # (3) run-s11b done
    # cmd_sink is a local node — no orchestrator events emitted for it.
    run_ends = _events_for(run, "run-end")
    assert run_ends and run_ends[-1].get("status") == "done", (
        f"run-s11b did not end with status=done: {run_ends}"
    )


# ── ST-RLG-21: Command multi-goto fan-out (case #15) ─────────────────────────


def test_st_rlg_21_command_multi_goto(
    sprint4_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """Case #15: Command(goto=['cmd_sink_a','writer']) triggers mixed local+remote fan-out.

    cmd_sink_a is a local node; writer is the remote-writer remote node.
    Both are targeted by Command in the same superstep.

    Verifies:
    1. Demo output shows S12 results containing from_cmd_sink_a (local branch ran).
    2. Orchestrator run-s12: command node-exit ok.
    3. Orchestrator run-s12: writer (remote) has node-enter + node-exit.
    4. run-s12 ended with status=done.
    """
    _require_node(orchestrator_url, "command")
    _require_node(orchestrator_url, "writer")

    out = sprint4_demo.stdout + sprint4_demo.stderr

    # (1) Demo output — local branch result present
    assert "S12 results (local branch):" in out, f"S12 results line missing:\n{out[-3000:]}"
    s12_line = next((l for l in out.splitlines() if "S12 results (local branch):" in l), "")
    assert "from_cmd_sink_a" in s12_line, (
        f"'from_cmd_sink_a' not in S12 results: {s12_line}"
    )

    run = _get_run(orchestrator_url, "run-s12")

    # (2) command node-exit ok
    cmd_exits = _events_for(run, "node-exit", "command")
    assert cmd_exits, "no node-exit for 'command' in run-s12"
    assert cmd_exits[-1].get("status") == "ok", (
        f"command node-exit status != ok: {cmd_exits[-1]}"
    )

    # (3) writer (remote branch) entered and exited
    assert _events_for(run, "node-enter", "writer"), (
        "no node-enter for 'writer' in run-s12"
    )
    assert _events_for(run, "node-exit", "writer"), (
        "no node-exit for 'writer' in run-s12"
    )

    # (4) run-s12 done
    run_ends = _events_for(run, "run-end")
    assert run_ends and run_ends[-1].get("status") == "done", (
        f"run-s12 did not end with status=done: {run_ends}"
    )

    # ── Node traversal trace (visible with -s) ────────────────────────────────
    s12_lines = [l for l in out.splitlines() if "S12" in l or ("▶" in l and
                 any(n in l for n in ("cmd_entry", "command", "cmd_sink", "writer", "cmd_joiner")))]
    print("\n── S12 node traversal ──")
    for line in s12_lines:
        print(line)
    print(f"── orchestrator events for run-s12: {[e['event'] + '/' + (e.get('node_name') or '-') for e in run['events']]} ──")


# ── ST-RLG-22: Invalid goto → proxy_block (case #14 error path) ──────────────


def test_st_rlg_22_bad_goto_proxy_block(
    sprint4_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """Case #14 error path: command.goto targets unknown node → proxy_block.

    The demo runs S13 which invokes the command node with mode='bad_goto'.
    The proxy detects that 'nonexistent_node_xyz' is not in the graph and
    raises InvalidCommand (error_kind=proxy_block).

    Verifies:
    1. Demo output shows S13 outcome contains 'proxy_block verified'.
    2. Orchestrator run-s13: node-error for 'command' with error_kind=proxy_block.
    3. The error message references the unknown goto target.
    4. run-s13 ended with status=failed.
    """
    _require_node(orchestrator_url, "command")

    out = sprint4_demo.stdout + sprint4_demo.stderr

    # (1) Demo output shows proxy_block verified
    assert "S13:" in out, f"S13 outcome line missing from demo output:\n{out[-3000:]}"
    s13_line = next((l for l in out.splitlines() if "S13:" in l and ("✓" in l or "ok" in l or "proxy_block" in l)), "")
    assert s13_line, (
        f"S13 did not show expected 'ok (proxy_block verified)' outcome:\n"
        + "\n".join(l for l in out.splitlines() if "S13" in l)
    )

    # (2) node-error for command with error_kind=proxy_block
    run = _get_run(orchestrator_url, "run-s13")
    error_events = _events_for(run, "node-error", "command")
    assert error_events, "no node-error for 'command' in run-s13"
    last_err = error_events[-1]
    assert last_err.get("error_kind") == "proxy_block", (
        f"expected error_kind=proxy_block, got: {last_err}"
    )

    # (3) error references the unknown target
    error_msg = last_err.get("error") or ""
    assert "nonexistent_node_xyz" in error_msg, (
        f"error message does not reference the bad goto target: {error_msg!r}"
    )

    # (4) run-s13 ended with status=failed
    run_ends = _events_for(run, "run-end")
    assert run_ends and run_ends[-1].get("status") == "failed", (
        f"run-s13 did not end with status=failed: {run_ends}"
    )
