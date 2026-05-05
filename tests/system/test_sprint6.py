"""Sprint 6 system tests — ST-RLG-27: Thread persistence via checkpointer (Case 23).

All tests rely on the `sprint6_demo` session fixture (tests/conftest.py) which
starts the a2a-accumulator service and runs `main-langgraph` once, caching
the result.

Run against an already-up stack:

  AMAZEGRAPH_SKIP_COMPOSE=1 \\
  ORCHESTRATOR_URL=http://localhost:8011 \\
  JAEGER_URL=http://localhost:16696 \\
  /home/ubuntu/venv/bin/python -m pytest tests/system/test_sprint6.py -v
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
        pytest.skip(f"node '{node_name}' not registered; is a2a-accumulator running?")


# ── ST-RLG-27: Thread persistence via checkpointer (Case 23) ─────────────────


def test_st_rlg_27_thread_persistence(
    sprint6_demo: subprocess.CompletedProcess,
    orchestrator_url: str,
) -> None:
    """Case #23: checkpointer preserves state across two ainvoke() calls.

    Scenario S18: graph `start_s18 (local) → accumulator (remote)` is called
    twice with the same thread_id. The second call's node sees visits=1 from
    the checkpoint and returns visits=2.

    Verifies:
    1. Demo output shows S18 visits=2 and log=['run-1', 'run-2'].
    2. Orchestrator run-s18: two node-enter events for 'accumulator'
       (one per ainvoke turn).
    3. Orchestrator run-s18: two node-exit events with status=ok.
    4. run-s18 ended with status=done.
    """
    _require_node(orchestrator_url, "accumulator")

    out = sprint6_demo.stdout + sprint6_demo.stderr

    # 1. Demo output assertions
    assert "S18" in out, "S18 section missing from demo output"
    assert "visits=2" in out, (
        f"Expected visits=2 in S18 output. Relevant lines:\n"
        + "\n".join(l for l in out.splitlines() if "S18" in l or "visits" in l)
    )
    assert "run-1" in out and "run-2" in out, (
        "Expected 'run-1' and 'run-2' in S18 log output"
    )

    # 2+3. Run-event stream: two node-enter + two node-exit for accumulator
    run = _get_run(orchestrator_url, "run-s18")
    enters = _events_for(run, "node-enter", "accumulator")
    exits = _events_for(run, "node-exit", "accumulator")

    assert len(enters) == 2, (
        f"Expected 2 node-enter events for accumulator in run-s18, got {len(enters)}"
    )
    assert len(exits) == 2, (
        f"Expected 2 node-exit events for accumulator in run-s18, got {len(exits)}"
    )
    assert all(e.get("status") == "ok" for e in exits), (
        f"Not all node-exit events have status=ok: {exits}"
    )

    # 4. Run ended done
    run_ends = _events_for(run, "run-end")
    assert any(e.get("status") == "done" for e in run_ends), (
        f"run-s18 did not end with status=done. run-end events: {run_ends}"
    )
