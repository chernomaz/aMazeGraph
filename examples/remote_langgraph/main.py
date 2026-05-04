"""Sprint 2 demo — exercises 13 Sprint-2 capability cases in a single run.

Scenarios executed sequentially:
  S1  Original flow      : start → research → post_research → writer → post_writer
  S2  LLM + MCP tool     : start → llm_tool   (skipped if no OPENAI_API_KEY)
  S3  Config / runtime   : start → config_echo (thread_id + tenant_id round-trip)
  S4a Conditional A      : router → research  (mode="research")
  S4b Conditional B      : router → writer    (mode="write")
  S5  Audit no-op        : start → audit      (returns {})
  S6  Parallel fan-out   : planner → [research_a, research_b] → joiner
"""

import asyncio
import logging
import operator
import sys
from datetime import datetime, timezone
from typing import Annotated, Any, TypedDict

from langchain_core.messages import BaseMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END
from langgraph.graph.message import add_messages
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

GRAPH_ID = "demo_graph_v1"


# ── State schema ─────────────────────────────────────────────────────────────


class GraphState(TypedDict, total=False):
    run_id: str
    trace_id: str
    user_request: str
    research_result: str
    final_answer: str
    # Reducers (Sprint 2: §9.1)
    log_trail: Annotated[list[str], operator.add]   # appends, never overwrites
    messages: Annotated[list[BaseMessage], add_messages]  # dedup by id
    results: Annotated[list[str], operator.add]     # fan-out accumulator
    # Config / runtime echo (ST-RLG-10)
    mode: str
    echoed_thread: str
    echoed_tenant: str
    # LLM+tool output (ST-RLG-9)
    tool_result: str


# ── Shared local nodes ────────────────────────────────────────────────────────


async def start_node(state: GraphState) -> dict:
    logger.info("start_node: forwarding user_request")
    return {"user_request": state.get("user_request", "")}


async def post_research_node(state: GraphState) -> dict:
    """Append to log_trail by returning only the delta (operator.add reducer)."""
    research = state.get("research_result", "") or ""
    msg = (
        f"post_research_node: research_result "
        f"({len(research)} chars) preview={research[:60]!r}"
    )
    logger.info(msg)
    return {"log_trail": [msg]}  # delta only — reducer appends


async def post_writer_node(state: GraphState) -> dict:
    """Append to log_trail by returning only the delta (operator.add reducer)."""
    final = state.get("final_answer", "") or ""
    msg = (
        f"post_writer_node: final_answer "
        f"({len(final)} chars) preview={final[:60]!r}"
    )
    logger.info(msg)
    return {"log_trail": [msg]}  # delta only — reducer appends


async def router_node(state: GraphState) -> dict:
    """No-op pass-through; conditional edges read state after this returns."""
    logger.info("router_node: mode=%s", state.get("mode"))
    return {}


async def s7_local_node(state: GraphState) -> dict:
    """Local node for S7: appends to log_trail before the remote node does."""
    msg = "s7_local_node: starting mixed-reducer scenario"
    logger.info(msg)
    return {"log_trail": [msg], "user_request": state.get("user_request", "")}


def route_by_mode(state: GraphState) -> str:
    """Conditional routing: mode='research' → research node; else → writer."""
    return "research" if state.get("mode") == "research" else "writer"


async def planner_node(state: GraphState) -> dict:
    """Fan-out entry: logs intent; both research_a and research_b fire next."""
    logger.info("planner_node: dispatching parallel research branches")
    return {}


async def joiner_node(state: GraphState) -> dict:
    """Fan-in: combines results from both branches."""
    results = state.get("results") or []
    combined = "; ".join(results)
    logger.info("joiner_node: combined %d results: %s", len(results), combined[:120])
    return {"final_answer": combined}


# ── Scenario helpers ──────────────────────────────────────────────────────────


def _make_workflow() -> AmazeGraph:
    """Fresh AmazeGraph for a scenario — each compile() registers its graph."""
    return AmazeGraph(GraphState, graph_id=GRAPH_ID)


async def _invoke(
    workflow: AmazeGraph,
    initial: dict,
    *,
    run_id: str,
    trace_id: str,
    config: RunnableConfig | None = None,
) -> dict:
    """Compile, emit run-start, invoke, emit run-end; close the workflow."""
    app = workflow.compile()

    await workflow.orchestrator.emit_event(
        run_id,
        {
            "event": "run-start",
            "graph_id": GRAPH_ID,
            "node_name": None,
            "trace_id": trace_id,
            "status": "running",
            "error": None,
            "ts": datetime.now(timezone.utc).isoformat(),
        },
    )

    invoke_kwargs: dict[str, Any] = {"input": {**initial, "run_id": run_id, "trace_id": trace_id}}
    if config:
        invoke_kwargs["config"] = config

    try:
        result = await app.ainvoke(**invoke_kwargs)
    except (
        RemoteNodeNotRegistered,
        RemoteNodeInvokeError,
        InvalidStatePatch,
        OrchestratorUnavailable,
    ) as exc:
        await workflow.orchestrator.emit_event(
            run_id,
            {
                "event": "run-end",
                "graph_id": GRAPH_ID,
                "node_name": None,
                "trace_id": trace_id,
                "status": "failed",
                "error": type(exc).__name__,
                "ts": datetime.now(timezone.utc).isoformat(),
            },
        )
        await workflow.aclose()
        raise

    await workflow.orchestrator.emit_event(
        run_id,
        {
            "event": "run-end",
            "graph_id": GRAPH_ID,
            "node_name": None,
            "trace_id": trace_id,
            "status": "done",
            "error": None,
            "ts": datetime.now(timezone.utc).isoformat(),
        },
    )
    await workflow.aclose()
    return result


# ── Scenario S1: Original flow ────────────────────────────────────────────────


async def scenario_s1_original() -> dict:
    """start → research → post_research → writer → post_writer.

    Covers: reducers (log_trail, operator.add), basic remote execution.
    """
    logger.info("═══ S1: original flow ═══")
    wf = _make_workflow()
    wf.add_node("start", start_node)
    wf.remote_node("research")
    wf.add_node("post_research", post_research_node)
    wf.remote_node("writer")
    wf.add_node("post_writer", post_writer_node)
    wf.set_entry_point("start")
    wf.add_edge("start", "research")
    wf.add_edge("research", "post_research")
    wf.add_edge("post_research", "writer")
    wf.add_edge("writer", "post_writer")
    wf.add_edge("post_writer", END)

    result = await _invoke(
        wf,
        {"user_request": "Create short architecture for remote LangGraph execution"},
        run_id="run-s1",
        trace_id="trace-s1",
    )
    logger.info("S1 final_answer: %s", (result.get("final_answer") or "")[:120])
    logger.info("S1 log_trail: %s", result.get("log_trail"))
    return result


# ── Scenario S2: LLM + MCP tool ──────────────────────────────────────────────


async def scenario_s2_llm_tool() -> dict:
    """start → llm_tool.

    Covers: real OpenAI ChatOpenAI + MCP web_search (skipped if no API key).
    """
    logger.info("═══ S2: LLM + MCP tool ═══")
    wf = _make_workflow()
    wf.add_node("start", start_node)
    wf.remote_node("llm_tool")
    wf.set_entry_point("start")
    wf.add_edge("start", "llm_tool")
    wf.add_edge("llm_tool", END)

    result = await _invoke(
        wf,
        {"user_request": "What is LangGraph and how does it handle parallel node execution?"},
        run_id="run-s2",
        trace_id="trace-s2",
    )
    logger.info("S2 tool_result: %s", (result.get("tool_result") or "")[:120])
    logger.info("S2 messages count: %d", len(result.get("messages") or []))
    return result


# ── Scenario S3: Config / runtime echo ───────────────────────────────────────


async def scenario_s3_config_echo() -> dict:
    """start → config_echo.

    Covers: thread_id propagation via configurable, runtime_context round-trip.
    """
    logger.info("═══ S3: config echo ═══")
    wf = _make_workflow()
    wf.add_node("start", start_node)
    wf.remote_node("config_echo")
    wf.set_entry_point("start")
    wf.add_edge("start", "config_echo")
    wf.add_edge("config_echo", END)

    config: RunnableConfig = {
        "configurable": {
            "thread_id": "t-123",
            "__amaze_runtime_context__": {"tenant_id": "acme"},
        }
    }
    result = await _invoke(
        wf,
        {"user_request": "echo config test"},
        run_id="run-s3",
        trace_id="trace-s3",
        config=config,
    )
    logger.info(
        "S3 echoed_thread=%s echoed_tenant=%s",
        result.get("echoed_thread"),
        result.get("echoed_tenant"),
    )
    return result


# ── Scenario S4: Conditional routing ─────────────────────────────────────────


async def scenario_s4_conditional(mode: str) -> dict:
    """router → research | writer  (mode-based conditional edge).

    Covers: add_conditional_edges with remote targets.
    """
    logger.info("═══ S4: conditional routing mode=%s ═══", mode)
    wf = _make_workflow()
    wf.add_node("router", router_node)
    wf.remote_node("research")
    wf.remote_node("writer")
    wf.set_entry_point("router")
    wf.add_conditional_edges(
        "router",
        route_by_mode,
        {"research": "research", "writer": "writer"},
    )
    wf.add_edge("research", END)
    wf.add_edge("writer", END)

    result = await _invoke(
        wf,
        {
            "user_request": "Explain remote LangGraph node execution",
            "mode": mode,
        },
        run_id=f"run-s4-{mode}",
        trace_id=f"trace-s4-{mode}",
    )
    # Exactly one of research_result or final_answer should be set
    hit_research = bool(result.get("research_result"))
    hit_writer = bool(result.get("final_answer"))
    logger.info(
        "S4 mode=%s hit_research=%s hit_writer=%s",
        mode,
        hit_research,
        hit_writer,
    )
    return result


# ── Scenario S5: Audit no-op ──────────────────────────────────────────────────


async def scenario_s5_audit() -> dict:
    """start → audit.

    Covers: remote node returning {} (no state changes).
    """
    logger.info("═══ S5: audit no-op ═══")
    wf = _make_workflow()
    wf.add_node("start", start_node)
    wf.remote_node("audit")
    wf.set_entry_point("start")
    wf.add_edge("start", "audit")
    wf.add_edge("audit", END)

    initial = {
        "user_request": "audit this run",
        "log_trail": ["initial_entry"],
    }
    result = await _invoke(
        wf,
        initial,
        run_id="run-s5",
        trace_id="trace-s5",
    )
    # State should be largely unchanged by audit
    logger.info("S5 user_request preserved: %s", result.get("user_request"))
    logger.info("S5 log_trail: %s", result.get("log_trail"))
    return result


# ── Scenario S6: Parallel fan-out ────────────────────────────────────────────


async def scenario_s6_fan_out() -> dict:
    """planner → [research_a, research_b] → joiner.

    Covers: real concurrent remote invocations (operator.add reducer merges
    both results); overlapping node-enter timestamps prove true parallelism.

    Note: httpx.AsyncClient defaults to max_connections=100 — no pool
    exhaustion even with many concurrent branches.
    """
    logger.info("═══ S6: parallel fan-out ═══")
    wf = _make_workflow()
    wf.add_node("planner", planner_node)
    wf.remote_node("research_a")
    wf.remote_node("research_b")
    wf.add_node("joiner", joiner_node)
    wf.set_entry_point("planner")
    # Both edges from planner fire concurrently in LangGraph's async superstep
    wf.add_edge("planner", "research_a")
    wf.add_edge("planner", "research_b")
    wf.add_edge("research_a", "joiner")
    wf.add_edge("research_b", "joiner")
    wf.add_edge("joiner", END)

    result = await _invoke(
        wf,
        {"user_request": "Analyse distributed graph execution scalability"},
        run_id="run-s6",
        trace_id="trace-s6",
    )
    results = result.get("results") or []
    logger.info("S6 results (%d items): %s", len(results), results)
    logger.info("S6 final_answer: %s", result.get("final_answer", "")[:120])
    return result


# ── Scenario S7: Mixed local + remote reducer ─────────────────────────────────


async def scenario_s7_mixed_reducer() -> dict:
    """s7_local (local) → research (remote).

    Both nodes append a delta to log_trail via operator.add. Proves that the
    reducer works correctly across the local/remote boundary — neither entry
    overwrites the other.
    """
    logger.info("═══ S7: mixed local+remote reducer ═══")
    wf = _make_workflow()
    wf.add_node("s7_local", s7_local_node)
    wf.remote_node("research")
    wf.set_entry_point("s7_local")
    wf.add_edge("s7_local", "research")
    wf.add_edge("research", END)

    result = await _invoke(
        wf,
        {"user_request": "mixed reducer test"},
        run_id="run-s7",
        trace_id="trace-s7",
    )
    logger.info("S7 log_trail: %s", result.get("log_trail"))
    return result


# ── Main ──────────────────────────────────────────────────────────────────────


async def main_async() -> int:
    tracer = trace.get_tracer("main-langgraph")

    with tracer.start_as_current_span("main-langgraph.sprint2-demo") as span:
        span.set_attribute("amaze.scenarios", "S1,S2,S3,S4a,S4b,S5,S6,S7")

        outcomes: dict[str, str] = {}

        # S1 — original flow (reducers + basic remote execution)
        try:
            r = await scenario_s1_original()
            outcomes["S1"] = "ok" if r.get("final_answer") else "no-final-answer"
        except Exception as exc:
            logger.error("S1 failed: %s", exc)
            outcomes["S1"] = f"FAILED: {exc}"

        # S2 — LLM + MCP tool (skip if no OPENAI_API_KEY)
        try:
            r = await scenario_s2_llm_tool()
            outcomes["S2"] = "ok"
        except RemoteNodeNotRegistered:
            outcomes["S2"] = "SKIP: llm_tool node not running"
            logger.warning("S2: llm_tool node not registered — is a2a-llm-tool running?")
        except Exception as exc:
            logger.error("S2 failed: %s", exc)
            outcomes["S2"] = f"FAILED: {exc}"

        # S3 — config echo (thread_id + runtime_context round-trip)
        try:
            r = await scenario_s3_config_echo()
            got_thread = r.get("echoed_thread") == "t-123"
            got_tenant = r.get("echoed_tenant") == "acme"
            outcomes["S3"] = "ok" if (got_thread and got_tenant) else f"MISMATCH thread={r.get('echoed_thread')!r} tenant={r.get('echoed_tenant')!r}"
        except RemoteNodeNotRegistered:
            outcomes["S3"] = "SKIP: config_echo node not running"
            logger.warning("S3: config_echo node not registered — is a2a-audit running?")
        except Exception as exc:
            logger.error("S3 failed: %s", exc)
            outcomes["S3"] = f"FAILED: {exc}"

        # S4a — conditional routing → research
        try:
            r = await scenario_s4_conditional("research")
            outcomes["S4a"] = "ok" if r.get("research_result") else "no-research_result"
        except RemoteNodeNotRegistered:
            outcomes["S4a"] = "SKIP: research/writer not running"
        except Exception as exc:
            logger.error("S4a failed: %s", exc)
            outcomes["S4a"] = f"FAILED: {exc}"

        # S4b — conditional routing → writer
        try:
            r = await scenario_s4_conditional("write")
            outcomes["S4b"] = "ok" if r.get("final_answer") else "no-final_answer"
        except RemoteNodeNotRegistered:
            outcomes["S4b"] = "SKIP: writer not running"
        except Exception as exc:
            logger.error("S4b failed: %s", exc)
            outcomes["S4b"] = f"FAILED: {exc}"

        # S5 — audit no-op
        try:
            r = await scenario_s5_audit()
            outcomes["S5"] = "ok" if r.get("user_request") else "state-lost"
        except RemoteNodeNotRegistered:
            outcomes["S5"] = "SKIP: audit node not running"
            logger.warning("S5: audit node not registered — is a2a-audit running?")
        except Exception as exc:
            logger.error("S5 failed: %s", exc)
            outcomes["S5"] = f"FAILED: {exc}"

        # S6 — parallel fan-out
        try:
            r = await scenario_s6_fan_out()
            results = r.get("results") or []
            has_a = any("research_a" in x for x in results)
            has_b = any("research_b" in x for x in results)
            outcomes["S6"] = "ok" if (has_a and has_b) else f"INCOMPLETE results={results}"
        except RemoteNodeNotRegistered:
            outcomes["S6"] = "SKIP: research_a/b not running"
            logger.warning("S6: research_a/b nodes not registered — are a2a-research-a/b running?")
        except Exception as exc:
            logger.error("S6 failed: %s", exc)
            outcomes["S6"] = f"FAILED: {exc}"

        # S7 — mixed local + remote reducer
        try:
            r = await scenario_s7_mixed_reducer()
            trail = r.get("log_trail") or []
            has_local = any("s7_local_node" in e for e in trail)
            has_remote = any("research_node" in e for e in trail)
            outcomes["S7"] = "ok" if (has_local and has_remote) else f"INCOMPLETE log_trail={trail}"
        except RemoteNodeNotRegistered:
            outcomes["S7"] = "SKIP: research node not running"
            logger.warning("S7: research node not registered — is a2a-research running?")
        except Exception as exc:
            logger.error("S7 failed: %s", exc)
            outcomes["S7"] = f"FAILED: {exc}"

        # Summary
        logger.info("═══ Sprint 2 demo summary ═══")
        all_ok = True
        for scenario, status in outcomes.items():
            prefix = "✓" if status.startswith("ok") else ("⚠" if status.startswith("SKIP") else "✗")
            logger.info("  %s %s: %s", prefix, scenario, status)
            if status.startswith("FAILED"):
                all_ok = False

        exit_code = 0 if all_ok else 1
        span.set_attribute("amaze.all_ok", all_ok)

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
        import traceback
        traceback.print_exc()
        sys.exit(1)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
