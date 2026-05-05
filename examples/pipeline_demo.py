"""Pipeline demo — local→remote→local Command→Send to researcher+writer.

Graph:
    setup (local)
        ↓
    planner (remote)     ← produces research_query, writing_brief, research_notes
        ↓
    dispatcher (local)   ← returns Command(goto=[Send("research",...), Send("writer",...)])
        ↙                                    ↘
  research (remote)                      writer (remote)
  receives custom query                  receives writing brief + placeholder
        ↘                                    ↙
                        END

Usage
-----
Run the stack:

    docker compose -p amazegraph-pipeline \\
        -f docker/compose.remote-langgraph.yml \\
        --profile pipeline \\
        up -d --build

    AMAZEGRAPH_SKIP_COMPOSE=1 \\
    AMAZE_ORCHESTRATOR_URL=http://localhost:8011 \\
    /home/ubuntu/venv/bin/python -m examples.pipeline_demo "LangGraph remote node execution"

Or run the nodes manually (three terminals):

    # terminal 1 — planner
    A2A_NODE_PORT=9020 A2A_NODE_PUBLIC_ENDPOINT=http://localhost:9020/invoke \\
        /home/ubuntu/venv/bin/python -m examples.a2a_nodes.planner_node

    # terminal 2 — researcher (already on port 9002 if main stack is up)
    # terminal 3 — writer    (already on port 9003 if main stack is up)

    # terminal 4 — run the demo
    AMAZE_ORCHESTRATOR_URL=http://localhost:8011 \\
        /home/ubuntu/venv/bin/python -m examples.pipeline_demo "distributed tracing"
"""

from __future__ import annotations

import asyncio
import logging
import operator
import sys
from datetime import datetime, timezone
from typing import Annotated, TypedDict

from langgraph.graph import END
from langgraph.types import Command, Send

from examples.a2a_nodes._common import setup_logging
from sdk.amaze import AmazeGraph, InvalidCommand, InvalidStatePatch, RemoteNodeInvokeError

logger = logging.getLogger(__name__)

GRAPH_ID = "demo_graph_v1"
RUN_ID = "run-pipeline"
TRACE_ID = "trace-pipeline"


# ── State ─────────────────────────────────────────────────────────────────────


class PipelineState(TypedDict, total=False):
    run_id: str
    trace_id: str
    topic: str
    # set by planner (remote node2)
    research_query: str
    writing_brief: str
    research_notes: str
    # set by research + writer (via Send, in parallel)
    research_result: str
    final_answer: str
    log_trail: Annotated[list[str], operator.add]


# ── Local nodes ───────────────────────────────────────────────────────────────


async def setup_node(state: PipelineState) -> dict:
    topic = state.get("topic", "distributed systems")
    logger.info("━━━ [setup] topic=%r", topic)
    return {"topic": topic}


async def dispatcher_node(state: PipelineState) -> Command:
    research_query = state.get("research_query", "")
    writing_brief = state.get("writing_brief", "")
    research_notes = state.get("research_notes", "")
    topic = state.get("topic", "")

    # Build custom per-branch payloads — each target node receives only these keys
    research_payload = {
        "user_request": f"{research_query}. {research_notes}",
    }
    writer_payload = {
        "user_request": writing_brief,
        # Writer runs in parallel with researcher; seed it with context from
        # the topic so it can draw on its own knowledge without waiting.
        "research_result": f"[parallel mode] Write from your own knowledge about: {topic}",
    }

    logger.info("━━━ [dispatcher] sending to research: %r", research_payload["user_request"][:80])
    logger.info("━━━ [dispatcher] sending to writer:   %r", writer_payload["user_request"][:80])

    return Command(
        goto=[
            Send("research", research_payload),
            Send("writer", writer_payload),
        ]
    )


# ── Graph builder ──────────────────────────────────────────────────────────────


def build_graph() -> AmazeGraph:
    wf = AmazeGraph(PipelineState, graph_id=GRAPH_ID)

    wf.add_node("setup", setup_node)
    wf.remote_node("planner")
    wf.add_node("dispatcher", dispatcher_node)
    wf.remote_node("research")
    wf.remote_node("writer")

    wf.set_entry_point("setup")
    wf.add_edge("setup", "planner")
    wf.add_edge("planner", "dispatcher")
    # No static edges from dispatcher — Send takes over routing
    wf.add_edge("research", END)
    wf.add_edge("writer", END)

    return wf


# ── Runner ────────────────────────────────────────────────────────────────────


async def run(topic: str) -> int:
    wf = build_graph()
    app = wf.compile()

    await wf.orchestrator.emit_event(
        RUN_ID,
        {
            "event": "run-start",
            "graph_id": GRAPH_ID,
            "node_name": None,
            "trace_id": TRACE_ID,
            "status": "running",
            "error": None,
            "ts": datetime.now(timezone.utc).isoformat(),
        },
    )

    logger.info("━━━ invoking pipeline for topic=%r", topic)
    try:
        result = await app.ainvoke(
            {"topic": topic, "run_id": RUN_ID, "trace_id": TRACE_ID}
        )
    except (RemoteNodeInvokeError, InvalidStatePatch, InvalidCommand) as exc:
        logger.error("pipeline failed: %s: %s", type(exc).__name__, exc)
        await wf.orchestrator.emit_event(
            RUN_ID,
            {
                "event": "run-end",
                "graph_id": GRAPH_ID,
                "node_name": None,
                "trace_id": TRACE_ID,
                "status": "failed",
                "error": type(exc).__name__,
                "ts": datetime.now(timezone.utc).isoformat(),
            },
        )
        await wf.aclose()
        return 1

    await wf.orchestrator.emit_event(
        RUN_ID,
        {
            "event": "run-end",
            "graph_id": GRAPH_ID,
            "node_name": None,
            "trace_id": TRACE_ID,
            "status": "done",
            "error": None,
            "ts": datetime.now(timezone.utc).isoformat(),
        },
    )
    await wf.aclose()

    # ── Results ───────────────────────────────────────────────────────────────
    sep = "─" * 72
    logger.info(sep)
    logger.info("TOPIC           : %s", result.get("topic"))
    logger.info("RESEARCH QUERY  : %s", result.get("research_query", "")[:100])
    logger.info("WRITING BRIEF   : %s", result.get("writing_brief", "")[:100])
    logger.info(sep)
    logger.info("RESEARCH RESULT :\n  %s", (result.get("research_result") or "—")[:400])
    logger.info(sep)
    logger.info("FINAL ANSWER    :\n  %s", (result.get("final_answer") or "—")[:400])
    logger.info(sep)
    return 0


def main() -> None:
    setup_logging("pipeline-demo")
    topic = " ".join(sys.argv[1:]) or "LangGraph remote node execution"
    exit_code = asyncio.run(run(topic))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
