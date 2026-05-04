"""Minimal S7 demo: mixed local + remote reducer.

Graph: s7_local (LOCAL) → research (REMOTE) → END

Both nodes append a delta to log_trail via operator.add.
Run with:
    AMAZE_ORCHESTRATOR_URL=http://localhost:8011 \
    /home/ubuntu/venv/bin/python -m examples.remote_langgraph.main_s7
"""

from __future__ import annotations

import asyncio
import logging
import operator
from typing import Annotated, TypedDict

from langchain_core.messages import BaseMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END
from langgraph.graph.message import add_messages

from examples.a2a_nodes._common import setup_logging
from sdk.amaze import AmazeGraph, OrchestratorUnavailable, RemoteNodeInvokeError, RemoteNodeNotRegistered

logger = logging.getLogger(__name__)

GRAPH_ID = "demo_graph_v1"


class GraphState(TypedDict, total=False):
    run_id: str
    trace_id: str
    user_request: str
    research_result: str
    final_answer: str
    log_trail: Annotated[list[str], operator.add]
    messages: Annotated[list[BaseMessage], add_messages]
    results: Annotated[list[str], operator.add]


async def s7_local_node(state: GraphState) -> dict:
    msg = "s7_local_node: starting mixed-reducer scenario"
    logger.info(msg)
    return {"log_trail": [msg], "user_request": state.get("user_request", "")}


async def run_s7() -> None:
    wf = AmazeGraph(GraphState, graph_id=GRAPH_ID)
    wf.add_node("s7_local", s7_local_node)
    wf.remote_node("research")
    wf.set_entry_point("s7_local")
    wf.add_edge("s7_local", "research")
    wf.add_edge("research", END)

    app = wf.compile()

    await wf.orchestrator.emit_event(
        "run-s7",
        {"event": "run-start", "graph_id": GRAPH_ID, "node_name": None,
         "trace_id": "trace-s7", "status": "running", "ts": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()},
    )

    config: RunnableConfig = {"configurable": {"thread_id": "s7-thread"}}
    result = await app.ainvoke(
        {"user_request": "mixed reducer test", "run_id": "run-s7", "trace_id": "trace-s7"},
        config=config,
    )

    await wf.orchestrator.emit_event(
        "run-s7",
        {"event": "run-end", "graph_id": GRAPH_ID, "node_name": None,
         "trace_id": "trace-s7", "status": "done", "ts": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat()},
    )
    await wf.aclose()

    logger.info("S7 log_trail: %s", result.get("log_trail"))
    logger.info("S7 research_result: %s", (result.get("research_result") or "")[:120])

    trail = result.get("log_trail") or []
    has_local = any("s7_local_node" in e for e in trail)
    has_remote = any("research_node" in e for e in trail)
    if has_local and has_remote:
        logger.info("✓ PASS: both local and remote entries present in log_trail")
    else:
        logger.error("✗ FAIL: missing entries — has_local=%s has_remote=%s", has_local, has_remote)


def main() -> None:
    setup_logging("main-s7")
    asyncio.run(run_s7())


if __name__ == "__main__":
    main()
