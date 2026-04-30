from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timezone
from typing import TypedDict

from langgraph.graph import END
from opentelemetry import trace

from examples.a2a_nodes._common import setup_logging
from sdk.amaze import (
    AmazeGraph,
    InvalidStatePatch,
    OrchestratorUnavailable,
    RemoteNodeInvokeError,
    RemoteNodeNotRegistered,
)

logger = logging.getLogger(__name__)


class GraphState(TypedDict, total=False):
    run_id: str
    trace_id: str
    user_request: str
    research_result: str
    final_answer: str
    log_trail: list[str]


async def start_node(state: GraphState) -> dict:
    logger.info("start_node: forwarding user_request")
    return {"user_request": state["user_request"]}


async def post_research_node(state: GraphState) -> dict:
    research = state.get("research_result", "") or ""
    msg = (
        f"post_research_node: observed research_result "
        f"({len(research)} chars) preview={research[:80]!r}"
    )
    logger.info(msg)
    trail = list(state.get("log_trail") or [])
    trail.append(msg)
    return {"log_trail": trail}


async def post_writer_node(state: GraphState) -> dict:
    final = state.get("final_answer", "") or ""
    msg = (
        f"post_writer_node: observed final_answer "
        f"({len(final)} chars) preview={final[:80]!r}"
    )
    logger.info(msg)
    trail = list(state.get("log_trail") or [])
    trail.append(msg)
    return {"log_trail": trail}


async def main_async() -> int:
    workflow = AmazeGraph(GraphState, graph_id="demo_graph_v1")
    logger.info("orchestrator URL=%s", workflow.orchestrator_url)

    workflow.add_node("start", start_node)
    workflow.remote_node("research")
    workflow.add_node("post_research", post_research_node)
    workflow.remote_node("writer")
    workflow.add_node("post_writer", post_writer_node)

    workflow.set_entry_point("start")
    workflow.add_edge("start", "research")
    workflow.add_edge("research", "post_research")
    workflow.add_edge("post_research", "writer")
    workflow.add_edge("writer", "post_writer")
    workflow.add_edge("post_writer", END)

    try:
        app = workflow.compile()
    except OrchestratorUnavailable as exc:
        logger.error("OrchestratorUnavailable during compile: %s", exc)
        await workflow.aclose()
        raise

    logger.info("graph compiled")

    run_id = "run-1"
    trace_id = "trace-1"

    tracer = trace.get_tracer("main-langgraph")
    with tracer.start_as_current_span("main-langgraph.run") as span:
        span.set_attribute("amaze.run_id", run_id)
        span.set_attribute("amaze.trace_id", trace_id)

        await workflow.orchestrator.emit_event(
            run_id,
            {
                "event": "run-start",
                "graph_id": "demo_graph_v1",
                "node_name": None,
                "trace_id": trace_id,
                "status": "running",
                "error": None,
                "ts": datetime.now(timezone.utc).isoformat(),
            },
        )

        try:
            logger.info("invoking graph")
            result = await app.ainvoke(
                {
                    "run_id": run_id,
                    "trace_id": trace_id,
                    "user_request": "Create short architecture for remote LangGraph execution",
                }
            )
            logger.info("graph completed")
        except RemoteNodeNotRegistered as exc:
            logger.error(
                "RemoteNodeNotRegistered graph_id=%s node_name=%s",
                exc.graph_id,
                exc.node_name,
            )
            await workflow.orchestrator.emit_event(
                run_id,
                {
                    "event": "run-end",
                    "graph_id": "demo_graph_v1",
                    "node_name": None,
                    "trace_id": trace_id,
                    "status": "failed",
                    "error": "remote-node-not-registered",
                    "ts": datetime.now(timezone.utc).isoformat(),
                },
            )
            await workflow.aclose()
            raise
        except RemoteNodeInvokeError as exc:
            logger.error(
                "RemoteNodeInvokeError graph_id=%s node_name=%s status=%s",
                exc.graph_id,
                exc.node_name,
                exc.status,
            )
            await workflow.orchestrator.emit_event(
                run_id,
                {
                    "event": "run-end",
                    "graph_id": "demo_graph_v1",
                    "node_name": None,
                    "trace_id": trace_id,
                    "status": "failed",
                    "error": "remote-node-invoke-error",
                    "ts": datetime.now(timezone.utc).isoformat(),
                },
            )
            await workflow.aclose()
            raise
        except InvalidStatePatch as exc:
            logger.error(
                "InvalidStatePatch graph_id=%s node_name=%s",
                exc.graph_id,
                exc.node_name,
            )
            await workflow.orchestrator.emit_event(
                run_id,
                {
                    "event": "run-end",
                    "graph_id": "demo_graph_v1",
                    "node_name": None,
                    "trace_id": trace_id,
                    "status": "failed",
                    "error": "invalid-state-patch",
                    "ts": datetime.now(timezone.utc).isoformat(),
                },
            )
            await workflow.aclose()
            raise
        except OrchestratorUnavailable as exc:
            logger.error("OrchestratorUnavailable: %s", exc)
            await workflow.aclose()
            raise

        logger.info("FINAL RESULT: %s", result.get("final_answer"))
        logger.info("FULL STATE: %s", result)

        final_answer = result.get("final_answer")
        if final_answer:
            logger.info("final_answer present")
            exit_code = 0
        else:
            logger.error("final_answer missing from result")
            exit_code = 1

        await workflow.orchestrator.emit_event(
            run_id,
            {
                "event": "run-end",
                "graph_id": "demo_graph_v1",
                "node_name": None,
                "trace_id": trace_id,
                "status": "done" if exit_code == 0 else "failed",
                "error": None,
                "ts": datetime.now(timezone.utc).isoformat(),
            },
        )

    await workflow.aclose()
    return exit_code


def main() -> None:
    setup_logging("main-langgraph")
    try:
        from sdk.amaze.langgraph import _init_otel
        _init_otel("main-langgraph")
    except ImportError:
        pass

    try:
        exit_code = asyncio.run(main_async())
    except (
        RemoteNodeNotRegistered,
        RemoteNodeInvokeError,
        InvalidStatePatch,
        OrchestratorUnavailable,
    ):
        raise
    sys.exit(exit_code)


if __name__ == "__main__":
    try:
        main()
    except (
        RemoteNodeNotRegistered,
        RemoteNodeInvokeError,
        InvalidStatePatch,
        OrchestratorUnavailable,
    ):
        import traceback
        traceback.print_exc()
        sys.exit(1)
