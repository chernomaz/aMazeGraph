"""Sprint 2 + Sprint 3 demo — exercises LangGraph node capabilities in a single run.

Scenarios executed sequentially:
  S1  Original flow      : start → research → post_research → writer → post_writer
  S2  LLM + MCP tool     : start → llm_tool   (skipped if no OPENAI_API_KEY)
  S3  Config / runtime   : start → config_echo (thread_id + tenant_id round-trip)
  S4a Conditional A      : router → research  (mode="research")
  S4b Conditional B      : router → writer    (mode="write")
  S5  Audit no-op        : start → audit      (returns {})
  S6  Parallel fan-out   : planner → [research_a, research_b] → joiner
  S7  Mixed reducer      : s7_local (local) → research (remote)
  S8  Subgraph node      : start → subgraph (remote, internally runs 2-step StateGraph)
  S9  Schema split       : s9_local → schema_remote; private fields absent from output
  S10 Recursion metadata : counter (remote, loops 3×) with langgraph_step echo
"""

import asyncio
import logging
import operator
import os
import sys
from datetime import datetime, timezone
from typing import Annotated, Any, TypedDict

from langchain_core.messages import BaseMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END
from langgraph.graph.message import add_messages
from opentelemetry import trace

from sdk.amaze import setup_logging
from sdk.amaze import (
    AmazeGraph,
    InvalidCommand,
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
    # Sprint 3: recursion metadata (ST-RLG-17)
    count: int
    langgraph_step_echo: int
    # Sprint 4: Command routing (cases 14+15)
    cmd_result: str
    # Sprint 5: Send routing (cases 13+16)
    full_state_marker: str          # present in full state; absent from Send.arg
    send_received: dict             # what send_sink recorded from its input
    send_results: Annotated[list[str], operator.add]  # fan-out accumulator
    status: str                     # set by Command.update in with_update mode
    input: str                      # payload field forwarded to Send.arg
    # Sprint 6: thread persistence via checkpointing (S18)
    visits: int                     # incremented by accumulator on each turn
    log: Annotated[list[str], operator.add]  # appended per turn by accumulator
    # Sprint 7: cache demo (S20/S21/S22)
    cached_result: str              # returned by cached_node; embeds timestamp


# Sprint 3 — Schema split state schemas (case #28, ST-RLG-18)

class S9InputState(TypedDict, total=False):
    """Fields accepted at ainvoke() call site."""
    run_id: str
    trace_id: str
    user_request: str


class S9PrivateState(S9InputState, total=False):
    """Full internal graph state; extends InputState with private fields."""
    private_data: str
    log_trail: Annotated[list[str], operator.add]


class S9OutputState(TypedDict, total=False):
    """Fields returned by ainvoke()."""
    final_answer: str


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


async def cmd_entry_node(state: GraphState) -> dict:
    logger.info("▶ [cmd_entry] entered — mode=%s user_request=%s", state.get("mode"), (state.get("user_request") or "")[:40])
    return {}


async def cmd_sink_node(state: GraphState) -> dict:
    logger.info("▶ [cmd_sink] entered — cmd_result=%s", state.get("cmd_result"))
    return {"log_trail": [f"cmd_sink_node: received cmd_result={state.get('cmd_result')!r}"]}


async def cmd_sink_a_node(state: GraphState) -> dict:
    logger.info("▶ [cmd_sink_a] entered — appending from_cmd_sink_a to results")
    return {"results": ["from_cmd_sink_a"], "log_trail": ["cmd_sink_a_node: done"]}


async def cmd_sink_b_node(state: GraphState) -> dict:
    logger.info("▶ [cmd_sink_b] entered — appending from_cmd_sink_b to results")
    return {"results": ["from_cmd_sink_b"], "log_trail": ["cmd_sink_b_node: done"]}


async def send_sink_node(state: dict) -> dict:
    # Records exactly which keys arrived via Send.arg
    return {"send_received": dict(state)}


async def send_sink_a_node_s5(state: dict) -> dict:
    return {"send_results": [f"branch_a:{state.get('val', '')}:{state.get('branch', '')}"]}


async def send_sink_b_node_s5(state: dict) -> dict:
    return {"send_results": [f"branch_b:{state.get('val', '')}:{state.get('branch', '')}"]}



async def cmd_joiner_node(state: GraphState) -> dict:
    results = state.get("results") or []          # from local branches (operator.add reducer)
    writer_answer = state.get("final_answer") or ""  # from writer remote branch
    local_part = "; ".join(results)
    logger.info(
        "▶ [cmd_joiner] entered — local_results=%s  writer_final_answer=%s",
        results, writer_answer[:80],
    )
    merged = f"local=[{local_part}] | writer=[{writer_answer[:80]}]"
    return {"final_answer": merged}


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
        InvalidCommand,
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


# ── Sprint 3 local nodes ──────────────────────────────────────────────────────


async def s9_local_node(state: dict) -> dict:
    """Populate private_data before the remote schema_remote node runs."""
    secret = f"private:{state.get('user_request', '')[:40]}"
    logger.info("s9_local_node: setting private_data=%r", secret)
    return {"private_data": secret, "log_trail": ["s9_local_node: populated private_data"]}


def s10_route(state: dict) -> str:
    """Loop back to counter while count < 3; exit otherwise."""
    return "counter" if (state.get("count") or 0) < 3 else "__end__"


# ── Scenario S8: Subgraph node ────────────────────────────────────────────────


async def scenario_s8_subgraph() -> dict:
    """start → subgraph (remote).

    The remote node internally compiles and runs a 2-step StateGraph,
    then returns a merged state_patch. Demonstrates cases #19 + #20.
    """
    logger.info("═══ S8: subgraph node ═══")
    wf = _make_workflow()
    wf.add_node("start", start_node)
    wf.remote_node("subgraph")
    wf.set_entry_point("start")
    wf.add_edge("start", "subgraph")
    wf.add_edge("subgraph", END)

    result = await _invoke(
        wf,
        {"user_request": "test subgraph opaque node"},
        run_id="run-s8",
        trace_id="trace-s8",
    )
    logger.info("S8 research_result: %s", (result.get("research_result") or "")[:120])
    logger.info("S8 log_trail: %s", result.get("log_trail"))
    return result


# ── Scenario S9: Schema split ─────────────────────────────────────────────────


async def scenario_s9_schema_split() -> dict:
    """s9_local (local) → schema_remote (remote).

    Demonstrates case #28: AmazeGraph compiled with input/output schema.
    The ainvoke() result only contains OutputState fields (final_answer);
    private_data is filtered by LangGraph's output schema.
    """
    logger.info("═══ S9: schema split (input/private/output) ═══")
    wf = AmazeGraph(
        S9PrivateState,
        graph_id=GRAPH_ID,
        input=S9InputState,
        output=S9OutputState,
    )
    wf.add_node("s9_local", s9_local_node)
    wf.remote_node("schema_remote")
    wf.set_entry_point("s9_local")
    wf.add_edge("s9_local", "schema_remote")
    wf.add_edge("schema_remote", END)

    result = await _invoke(
        wf,
        {"user_request": "schema split test"},
        run_id="run-s9",
        trace_id="trace-s9",
    )
    logger.info("S9 final_answer: %s", result.get("final_answer", "")[:120])
    logger.info("S9 keys in result: %s", list(result.keys()))
    return result


# ── Scenario S10: Recursion / step metadata ───────────────────────────────────


async def scenario_s10_recursion_metadata() -> dict:
    """counter (remote) loops 3 times; each invocation echoes langgraph_step.

    Demonstrates case #25: langgraph_step metadata survives the wire
    and increments correctly across supersteps.
    """
    logger.info("═══ S10: recursion metadata (langgraph_step echo) ═══")
    wf = _make_workflow()
    wf.remote_node("counter")
    wf.set_entry_point("counter")
    wf.add_conditional_edges("counter", s10_route, {"counter": "counter", "__end__": END})

    config: RunnableConfig = {"configurable": {"thread_id": "s10-thread"}, "recursion_limit": 25}
    result = await _invoke(
        wf,
        {"user_request": "recursion step test", "count": 0},
        run_id="run-s10",
        trace_id="trace-s10",
        config=config,
    )
    logger.info(
        "S10 count=%s langgraph_step_echo=%s log_trail=%s",
        result.get("count"),
        result.get("langgraph_step_echo"),
        result.get("log_trail"),
    )
    return result


# ── Scenario S11: Command single-goto (Case 14) ───────────────────────────────


async def scenario_s11_command_single_goto() -> dict:
    """cmd_entry → command (remote, Command.goto='cmd_sink') → cmd_sink.

    Demonstrates case #14: remote node returns AmazeCommand driving routing.
    """
    logger.info("═══ S11: Command single-goto ═══")
    wf = _make_workflow()
    wf.add_node("cmd_entry", cmd_entry_node)
    wf.remote_node("command")
    wf.add_node("cmd_sink", cmd_sink_node)
    wf.set_entry_point("cmd_entry")
    wf.add_edge("cmd_entry", "command")
    # No static edge from command → cmd_sink; Command.goto handles routing.
    wf.add_edge("cmd_sink", END)

    result = await _invoke(
        wf,
        {"user_request": "command routing test", "mode": "single"},
        run_id="run-s11",
        trace_id="trace-s11",
    )
    logger.info("S11 cmd_result: %s", result.get("cmd_result"))
    logger.info("S11 log_trail: %s", result.get("log_trail"))
    return result


async def scenario_s11_command_update_goto() -> dict:
    """cmd_entry → command (remote, mode='update_goto') → cmd_sink.

    Same graph as S11 but command.update carries state + goto routes.
    """
    logger.info("═══ S11b: Command update+goto ═══")
    wf = _make_workflow()
    wf.add_node("cmd_entry", cmd_entry_node)
    wf.remote_node("command")
    wf.add_node("cmd_sink", cmd_sink_node)
    wf.set_entry_point("cmd_entry")
    wf.add_edge("cmd_entry", "command")
    wf.add_edge("cmd_sink", END)

    result = await _invoke(
        wf,
        {"user_request": "update and goto test", "mode": "update_goto"},
        run_id="run-s11b",
        trace_id="trace-s11b",
    )
    logger.info("S11b cmd_result: %s", result.get("cmd_result"))
    return result


# ── Scenario S12: Command multi-goto (Case 15) ────────────────────────────────


async def scenario_s12_command_multi_goto() -> dict:
    """cmd_entry → command (remote, Command.goto=['cmd_sink_a','cmd_sink_b']) → joiner.

    Demonstrates case #15: Command.goto list triggers parallel fan-out.
    Both sinks execute in the same superstep; results reducer merges them.
    """
    logger.info("═══ S12: Command multi-goto (fan-out) ═══")
    wf = _make_workflow()
    wf.add_node("cmd_entry", cmd_entry_node)
    wf.remote_node("command")
    wf.add_node("cmd_sink_a", cmd_sink_a_node)
    wf.remote_node("writer")           # remote node — runs in a2a-writer container
    wf.add_node("cmd_joiner", cmd_joiner_node)
    wf.set_entry_point("cmd_entry")
    wf.add_edge("cmd_entry", "command")
    # No static edges from command → sinks; Command.goto=["cmd_sink_a","writer"]
    wf.add_edge("cmd_sink_a", "cmd_joiner")
    wf.add_edge("writer", "cmd_joiner")
    wf.add_edge("cmd_joiner", END)

    result = await _invoke(
        wf,
        {"user_request": "parallel command routing test", "mode": "multi"},
        run_id="run-s12",
        trace_id="trace-s12",
    )
    results = result.get("results") or []
    logger.info("S12 results (local branch):  %s", results)
    logger.info("S12 final_answer (merged):   %s", result.get("final_answer", "")[:120])
    logger.info("S12 log_trail: %s", result.get("log_trail"))
    return result


# ── Scenario S13: bad goto → proxy_block (Case 14 error path) ────────────────


async def scenario_s13_bad_goto_proxy_block() -> dict:
    """cmd_entry → command (remote, mode='bad_goto') → proxy raises InvalidCommand.

    Demonstrates case #14 error path: the proxy validates command.goto against
    the registered node set and raises InvalidCommand for unknown targets.
    Expected outcome is InvalidCommand; the scenario catches it and returns
    {"proxy_block_verified": True} so main_async() can mark S13 as ok.
    """
    logger.info("═══ S13: bad goto → proxy_block (error path) ═══")
    wf = _make_workflow()
    wf.add_node("cmd_entry", cmd_entry_node)
    wf.remote_node("command")
    # 'nonexistent_node_xyz' is not registered — proxy will reject it
    wf.set_entry_point("cmd_entry")
    wf.add_edge("cmd_entry", "command")

    try:
        await _invoke(
            wf,
            {"user_request": "bad goto test", "mode": "bad_goto"},
            run_id="run-s13",
            trace_id="trace-s13",
        )
        logger.warning("S13: UNEXPECTED — no error raised for bad goto")
        return {"proxy_block_verified": False}
    except InvalidCommand:
        logger.info("S13: InvalidCommand raised as expected — proxy_block verified")
        return {"proxy_block_verified": True}


# ── Scenario S14: Send single (Case 13) ──────────────────────────────────────


async def scenario_s14_send_single() -> dict:
    """cmd_entry → send_dispatcher (remote, mode='single') → send_sink."""
    logger.info("═══ S14: Send single ═══")
    wf = _make_workflow()
    wf.add_node("cmd_entry", cmd_entry_node)
    wf.remote_node("send_dispatcher")
    wf.add_node("send_sink", send_sink_node)
    wf.set_entry_point("cmd_entry")
    wf.add_edge("cmd_entry", "send_dispatcher")
    wf.add_edge("send_sink", END)

    result = await _invoke(
        wf,
        {"input": "hello-send", "full_state_marker": "FULL_STATE", "mode": "single"},
        run_id="run-s14",
        trace_id="trace-s14",
    )
    logger.info("S14 send_received: %s", result.get("send_received"))
    return result


# ── Scenario S15: Send parallel fan-out (Case 13) ────────────────────────────


async def scenario_s15_send_parallel() -> dict:
    """cmd_entry → send_dispatcher (remote, mode='parallel') → [send_sink_a, send_sink_b]."""
    logger.info("═══ S15: Send parallel fan-out ═══")
    wf = _make_workflow()
    wf.add_node("cmd_entry", cmd_entry_node)
    wf.remote_node("send_dispatcher")
    wf.add_node("send_sink_a", send_sink_a_node_s5)
    wf.add_node("send_sink_b", send_sink_b_node_s5)
    wf.set_entry_point("cmd_entry")
    wf.add_edge("cmd_entry", "send_dispatcher")
    wf.add_edge("send_sink_a", END)
    wf.add_edge("send_sink_b", END)

    result = await _invoke(
        wf,
        {"input": "fan-out", "full_state_marker": "FULL_STATE", "mode": "parallel"},
        run_id="run-s15",
        trace_id="trace-s15",
    )
    logger.info("S15 send_results: %s", result.get("send_results"))
    return result


# ── Scenario S16: Command + Send (Case 16) ────────────────────────────────────


async def scenario_s16_send_with_update() -> dict:
    """cmd_entry → send_dispatcher (remote, mode='with_update') → send_sink."""
    logger.info("═══ S16: Command+Send (update + Send routing) ═══")
    wf = _make_workflow()
    wf.add_node("cmd_entry", cmd_entry_node)
    wf.remote_node("send_dispatcher")
    wf.add_node("send_sink", send_sink_node)
    wf.set_entry_point("cmd_entry")
    wf.add_edge("cmd_entry", "send_dispatcher")
    wf.add_edge("send_sink", END)

    result = await _invoke(
        wf,
        {"input": "update-and-send", "full_state_marker": "FULL_STATE", "mode": "with_update"},
        run_id="run-s16",
        trace_id="trace-s16",
    )
    logger.info("S16 status: %s  send_received: %s", result.get("status"), result.get("send_received"))
    return result


# ── Scenario S17: bare Send (no Command wrapper) ─────────────────────────────


async def scenario_s17_bare_send() -> dict:
    """cmd_entry → send_dispatcher (remote, mode='bare_send') → send_sink.

    Remote node returns Send(...) directly with no Command wrapper.
    Proxy normalises it to Command(goto=[Send(...)]) on the wire.
    """
    logger.info("═══ S17: bare Send (no Command wrapper) ═══")
    wf = _make_workflow()
    wf.add_node("cmd_entry", cmd_entry_node)
    wf.remote_node("send_dispatcher")
    wf.add_node("send_sink", send_sink_node)
    wf.set_entry_point("cmd_entry")
    wf.add_edge("cmd_entry", "send_dispatcher")
    wf.add_edge("send_sink", END)

    result = await _invoke(
        wf,
        {"input": "bare-send-test", "full_state_marker": "FULL_STATE", "mode": "bare_send"},
        run_id="run-s17",
        trace_id="trace-s17",
    )
    logger.info("S17 send_received: %s", result.get("send_received"))
    return result


# ── Scenario S18: Thread persistence via checkpointing ───────────────────────


async def start_s18_node(state: GraphState) -> dict:
    """Local entry node for S18 — passes state through unchanged."""
    logger.info("start_s18_node: forwarding to accumulator")
    return {}


async def scenario_s18_checkpointer(checkpointer) -> dict:
    """start_s18 (local) → accumulator (remote), called twice on the same thread.

    Demonstrates thread persistence: on the second ainvoke() the accumulator
    reads the persisted visits counter and appended log from the first turn.
    """
    logger.info("═══ S18: thread persistence via checkpointing ═══")

    wf = AmazeGraph(GraphState, graph_id=GRAPH_ID, checkpointer=checkpointer)
    wf.add_node("start_s18", start_s18_node)
    wf.remote_node("accumulator")
    wf.set_entry_point("start_s18")
    wf.add_edge("start_s18", "accumulator")
    wf.add_edge("accumulator", END)

    app = wf.compile()

    cfg: RunnableConfig = {"configurable": {"thread_id": "s18-thread"}}

    await wf.orchestrator.emit_event(
        "run-s18",
        {
            "event": "run-start",
            "graph_id": GRAPH_ID,
            "node_name": None,
            "trace_id": "trace-s18",
            "status": "running",
            "error": None,
            "ts": datetime.now(timezone.utc).isoformat(),
        },
    )

    result1 = await app.ainvoke(
        {"run_id": "run-s18", "trace_id": "trace-s18", "input": "run-1"},
        config=cfg,
    )
    logger.info("S18 run-1: visits=%s log=%s", result1.get("visits"), result1.get("log"))

    result2 = await app.ainvoke(
        {"run_id": "run-s18", "trace_id": "trace-s18", "input": "run-2"},
        config=cfg,
    )
    logger.info("S18 run-2: visits=%s log=%s", result2.get("visits"), result2.get("log"))

    assert result1.get("visits") == 1, f"S18 run-1 visits expected 1, got {result1.get('visits')}"
    assert result2.get("visits") == 2, f"S18 run-2 visits expected 2, got {result2.get('visits')}"
    assert result2.get("log", []) == ["run-1", "run-2"], (
        f"S18 run-2 log expected ['run-1','run-2'], got {result2.get('log')}"
    )

    await wf.orchestrator.emit_event(
        "run-s18",
        {
            "event": "run-end",
            "graph_id": GRAPH_ID,
            "node_name": None,
            "trace_id": "trace-s18",
            "status": "done",
            "error": None,
            "ts": datetime.now(timezone.utc).isoformat(),
        },
    )

    logger.info("S18 visits=%s log=%s", result2["visits"], result2.get("log"))
    await wf.aclose()
    return result2


# ── Scenario S19: LangSmith trace propagation ─────────────────────────────────


async def scenario_s19_langsmith_trace() -> dict:
    """Invoke llm_tool node with LangSmith tracing active.

    Demonstrates that parent_run_id is extracted from the driver's
    CallbackManager and sent to the remote node via the wire
    `langsmith_context` field.  The remote node reconstructs a
    LangChainTracer with the correct parent_run_id before calling the LLM,
    so LLM spans appear nested under the graph root in LangSmith.
    """
    logger.info("═══ S19: LangSmith trace propagation ═══")
    langsmith_enabled = os.environ.get("LANGCHAIN_TRACING_V2", "false").lower() == "true"
    if not langsmith_enabled:
        logger.info("S19: LANGCHAIN_TRACING_V2 not set — langsmith_context will be None on wire")
    wf = _make_workflow()
    wf.add_node("start", start_node)
    wf.remote_node("llm_tool")
    wf.set_entry_point("start")
    wf.add_edge("start", "llm_tool")
    wf.add_edge("llm_tool", END)

    result = await _invoke(
        wf,
        {"user_request": "What is LangGraph and how does it handle remote node execution?"},
        run_id="run-s19",
        trace_id="trace-s19",
    )
    logger.info("S19 tool_result: %s", (result.get("tool_result") or "")[:120])
    logger.info("S19 messages count: %d", len(result.get("messages") or []))
    return result


# ── Scenario S20: Cache hit ───────────────────────────────────────────────────


async def scenario_s20_cache_hit() -> dict:
    """Two calls with identical state within TTL window → second is a cache hit.

    Demonstrates orchestrator-side Redis cache (Case 22, ST-RLG-29).
    Both calls must return the identical cached_result string — the embedded
    timestamp proves the node was not re-invoked.
    """
    logger.info("═══ S20: cache hit (same input within TTL=2s) ═══")
    input_val = "cache-hit-test"

    async def _run_cached(run_suffix: str) -> dict:
        wf = _make_workflow()
        wf.remote_node("cached_node")
        wf.set_entry_point("cached_node")
        wf.add_edge("cached_node", END)
        return await _invoke(
            wf,
            {"input": input_val},
            run_id=f"run-s20-{run_suffix}",
            trace_id=f"trace-s20-{run_suffix}",
        )

    result1 = await _run_cached("first")
    result2 = await _run_cached("second")

    ts1 = result1.get("cached_result", "")
    ts2 = result2.get("cached_result", "")
    logger.info("S20 first  cached_result: %s", ts1)
    logger.info("S20 second cached_result: %s", ts2)
    logger.info("S20 cache hit verified (strings equal): %s", ts1 == ts2)
    return {"cached_result_1": ts1, "cached_result_2": ts2, "cache_hit": ts1 == ts2}


# ── Scenario S21: TTL expiry ──────────────────────────────────────────────────


async def scenario_s21_ttl_expiry() -> dict:
    """First call, sleep 3s (TTL=2s), third call — expiry produces a fresh result.

    Demonstrates that cache entries expire after TTL seconds (ST-RLG-30).
    Pass condition: first and third cached_result strings differ.
    """
    logger.info("═══ S21: TTL expiry (sleep 3s > TTL=2s) ═══")
    input_val = "ttl-expiry-test"

    async def _run_cached(run_suffix: str) -> dict:
        wf = _make_workflow()
        wf.remote_node("cached_node")
        wf.set_entry_point("cached_node")
        wf.add_edge("cached_node", END)
        return await _invoke(
            wf,
            {"input": input_val},
            run_id=f"run-s21-{run_suffix}",
            trace_id=f"trace-s21-{run_suffix}",
        )

    result1 = await _run_cached("first")
    logger.info("S21 sleeping 3s to outlast TTL=2s ...")
    await asyncio.sleep(3)
    result3 = await _run_cached("third")

    ts1 = result1.get("cached_result", "")
    ts3 = result3.get("cached_result", "")
    logger.info("S21 first cached_result:  %s", ts1)
    logger.info("S21 third cached_result:  %s", ts3)
    logger.info("S21 expiry verified (strings differ): %s", ts1 != ts3)
    return {"cached_result_1": ts1, "cached_result_3": ts3, "expiry_verified": ts1 != ts3}


# ── Scenario S22: Key scoping ─────────────────────────────────────────────────


async def scenario_s22_key_scoping() -> dict:
    """Different state inputs → different cache keys; repeat first → cache hit.

    Demonstrates that the cache key is scoped by state, so two calls with
    different `input` values do not share a cached entry, but a third call
    that repeats the first input (within TTL) IS a cache hit (ST-RLG-31).
    """
    logger.info("═══ S22: key scoping (different states → different cache keys) ═══")

    async def _run_cached(input_val: str, run_suffix: str) -> dict:
        wf = _make_workflow()
        wf.remote_node("cached_node")
        wf.set_entry_point("cached_node")
        wf.add_edge("cached_node", END)
        return await _invoke(
            wf,
            {"input": input_val},
            run_id=f"run-s22-{run_suffix}",
            trace_id=f"trace-s22-{run_suffix}",
        )

    result_a1 = await _run_cached("scope-input-A", "a1")
    result_b  = await _run_cached("scope-input-B", "b")
    result_a2 = await _run_cached("scope-input-A", "a2")  # within TTL → cache hit

    ts_a1 = result_a1.get("cached_result", "")
    ts_b  = result_b.get("cached_result", "")
    ts_a2 = result_a2.get("cached_result", "")

    diff_inputs  = ts_a1 != ts_b
    same_repeat  = ts_a1 == ts_a2
    logger.info("S22 input-A first:  %s", ts_a1)
    logger.info("S22 input-B:        %s", ts_b)
    logger.info("S22 input-A repeat: %s", ts_a2)
    logger.info("S22 different inputs → different results: %s", diff_inputs)
    logger.info("S22 repeat within TTL → cache hit:        %s", same_repeat)
    return {
        "ts_a1": ts_a1, "ts_b": ts_b, "ts_a2": ts_a2,
        "diff_inputs": diff_inputs, "same_repeat": same_repeat,
    }


# ── Main ──────────────────────────────────────────────────────────────────────


async def main_async() -> int:
    tracer = trace.get_tracer("main-langgraph")

    with tracer.start_as_current_span("main-langgraph.sprint7-demo") as span:
        span.set_attribute("amaze.scenarios", "S1,S2,S3,S4a,S4b,S5,S6,S7,S8,S9,S10,S11,S11b,S12,S13,S14,S15,S16,S17,S18,S19,S20,S21,S22")

        outcomes: dict[str, str] = {}

        # S1 — original flow (reducers + basic remote execution)
        try:
            r = await scenario_s1_original()
            outcomes["S1"] = "ok" if r.get("final_answer") else "no-final-answer"
        except Exception as exc:
            logger.error("S1 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S1"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S2 — LLM + MCP tool (skip if no OPENAI_API_KEY)
        try:
            r = await scenario_s2_llm_tool()
            outcomes["S2"] = "ok"
        except RemoteNodeNotRegistered:
            outcomes["S2"] = "SKIP: llm_tool node not running"
            logger.warning("S2: llm_tool node not registered — is a2a-llm-tool running?")
        except Exception as exc:
            logger.error("S2 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S2"] = f"FAILED: {type(exc).__name__}: {exc}"

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
            logger.error("S3 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S3"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S4a — conditional routing → research
        try:
            r = await scenario_s4_conditional("research")
            outcomes["S4a"] = "ok" if r.get("research_result") else "no-research_result"
        except RemoteNodeNotRegistered:
            outcomes["S4a"] = "SKIP: research/writer not running"
        except Exception as exc:
            logger.error("S4a failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S4a"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S4b — conditional routing → writer
        try:
            r = await scenario_s4_conditional("write")
            outcomes["S4b"] = "ok" if r.get("final_answer") else "no-final_answer"
        except RemoteNodeNotRegistered:
            outcomes["S4b"] = "SKIP: writer not running"
        except Exception as exc:
            logger.error("S4b failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S4b"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S5 — audit no-op
        try:
            r = await scenario_s5_audit()
            outcomes["S5"] = "ok" if r.get("user_request") else "state-lost"
        except RemoteNodeNotRegistered:
            outcomes["S5"] = "SKIP: audit node not running"
            logger.warning("S5: audit node not registered — is a2a-audit running?")
        except Exception as exc:
            logger.error("S5 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S5"] = f"FAILED: {type(exc).__name__}: {exc}"

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
            logger.error("S6 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S6"] = f"FAILED: {type(exc).__name__}: {exc}"

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
            logger.error("S7 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S7"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S8 — subgraph node (cases #19 + #20)
        try:
            r = await scenario_s8_subgraph()
            res = r.get("research_result") or ""
            outcomes["S8"] = "ok" if ("step_a" in res and "step_b" in res) else f"INCOMPLETE research_result={res[:80]}"
        except RemoteNodeNotRegistered:
            outcomes["S8"] = "SKIP: subgraph node not running"
            logger.warning("S8: subgraph node not registered — is a2a-s3 running?")
        except Exception as exc:
            logger.error("S8 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S8"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S9 — schema split (case #28)
        try:
            r = await scenario_s9_schema_split()
            has_answer = bool(r.get("final_answer"))
            no_private = "private_data" not in r
            outcomes["S9"] = "ok" if (has_answer and no_private) else f"UNEXPECTED keys={list(r.keys())}"
        except RemoteNodeNotRegistered:
            outcomes["S9"] = "SKIP: schema_remote node not running"
            logger.warning("S9: schema_remote node not registered — is a2a-s3 running?")
        except Exception as exc:
            logger.error("S9 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S9"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S10 — recursion / step metadata (case #25)
        try:
            r = await scenario_s10_recursion_metadata()
            final_count = r.get("count") or 0
            step_echo = r.get("langgraph_step_echo")
            outcomes["S10"] = "ok" if (final_count >= 3 and step_echo is not None) else f"INCOMPLETE count={final_count} step_echo={step_echo}"
        except RemoteNodeNotRegistered:
            outcomes["S10"] = "SKIP: counter node not running"
            logger.warning("S10: counter node not registered — is a2a-s3 running?")
        except Exception as exc:
            logger.error("S10 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S10"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S11 — Command single-goto (case #14)
        try:
            r = await scenario_s11_command_single_goto()
            outcomes["S11"] = "ok" if r.get("cmd_result") == "single-goto-result" else f"UNEXPECTED cmd_result={r.get('cmd_result')!r}"
        except RemoteNodeNotRegistered:
            outcomes["S11"] = "SKIP: command node not running"
            logger.warning("S11: command node not registered — is a2a-command running?")
        except Exception as exc:
            logger.error("S11 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S11"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S11b — Command update+goto (case #14 variant)
        try:
            r = await scenario_s11_command_update_goto()
            cmd = r.get("cmd_result") or ""
            outcomes["S11b"] = "ok" if cmd.startswith("processed:") else f"UNEXPECTED cmd_result={cmd!r}"
        except RemoteNodeNotRegistered:
            outcomes["S11b"] = "SKIP: command node not running"
            logger.warning("S11b: command node not registered — is a2a-command running?")
        except Exception as exc:
            logger.error("S11b failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S11b"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S12 — Command multi-goto / fan-out (case #15)
        try:
            r = await scenario_s12_command_multi_goto()
            results = set(r.get("results") or [])
            # cmd_sink_a (local) appends to results; writer (remote) writes final_answer
            outcomes["S12"] = "ok" if "from_cmd_sink_a" in results else f"INCOMPLETE results={results}"
        except RemoteNodeNotRegistered:
            outcomes["S12"] = "SKIP: command node not running"
            logger.warning("S12: command node not registered — is a2a-command running?")
        except Exception as exc:
            logger.error("S12 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S12"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S13 — bad goto → proxy_block (case #14 error path)
        try:
            r = await scenario_s13_bad_goto_proxy_block()
            outcomes["S13"] = "ok (proxy_block verified)" if r.get("proxy_block_verified") else "UNEXPECTED: no error raised"
        except RemoteNodeNotRegistered:
            outcomes["S13"] = "SKIP: command node not running"
            logger.warning("S13: command node not registered — is a2a-command running?")
        except Exception as exc:
            logger.error("S13 failed unexpectedly [%s]: %s", type(exc).__name__, exc)
            outcomes["S13"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S14 — Send single (case #13)
        try:
            r = await scenario_s14_send_single()
            recv = r.get("send_received") or {}
            has_val = "val" in recv
            no_marker = "full_state_marker" not in recv
            outcomes["S14"] = "ok" if (has_val and no_marker) else f"UNEXPECTED send_received={recv}"
        except RemoteNodeNotRegistered:
            outcomes["S14"] = "SKIP: send_dispatcher not running"
            logger.warning("S14: send_dispatcher not registered — is a2a-send running?")
        except Exception as exc:
            logger.error("S14 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S14"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S15 — Send parallel fan-out (case #13)
        try:
            r = await scenario_s15_send_parallel()
            results = r.get("send_results") or []
            has_a = any("branch_a" in x for x in results)
            has_b = any("branch_b" in x for x in results)
            outcomes["S15"] = "ok" if (has_a and has_b) else f"INCOMPLETE send_results={results}"
        except RemoteNodeNotRegistered:
            outcomes["S15"] = "SKIP: send_dispatcher not running"
            logger.warning("S15: send_dispatcher not registered — is a2a-send running?")
        except Exception as exc:
            logger.error("S15 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S15"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S16 — Command+Send (case #16)
        try:
            r = await scenario_s16_send_with_update()
            recv = r.get("send_received") or {}
            outcomes["S16"] = "ok" if (r.get("status") == "dispatched" and "val" in recv) else f"UNEXPECTED status={r.get('status')!r} send_received={recv}"
        except RemoteNodeNotRegistered:
            outcomes["S16"] = "SKIP: send_dispatcher not running"
            logger.warning("S16: send_dispatcher not registered — is a2a-send running?")
        except Exception as exc:
            logger.error("S16 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S16"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S17 — bare Send (no Command wrapper)
        try:
            r = await scenario_s17_bare_send()
            recv = r.get("send_received") or {}
            outcomes["S17"] = "ok" if "val" in recv else f"UNEXPECTED send_received={recv}"
        except RemoteNodeNotRegistered:
            outcomes["S17"] = "SKIP: send_dispatcher not running"
            logger.warning("S17: send_dispatcher not registered — is a2a-send running?")
        except Exception as exc:
            logger.error("S17 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S17"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S18 — thread persistence via checkpointing (Sprint 6)
        try:
            from langgraph.checkpoint.redis.aio import AsyncRedisSaver

            redis_url = os.environ.get("AMAZE_REDIS_URL", "redis://localhost:6380")
            checkpointer = AsyncRedisSaver(redis_url=redis_url)
            await checkpointer.asetup()
            r = await scenario_s18_checkpointer(checkpointer)
            visits = r.get("visits")
            log = r.get("log", [])
            outcomes["S18"] = (
                "ok" if (visits == 2 and log == ["run-1", "run-2"])
                else f"UNEXPECTED visits={visits} log={log}"
            )
        except RemoteNodeNotRegistered:
            outcomes["S18"] = "SKIP: accumulator node not running"
            logger.warning("S18: accumulator node not registered — is a2a-accumulator running?")
        except Exception as exc:
            logger.error("S18 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S18"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S19 — LangSmith trace propagation (Sprint 7)
        try:
            r = await scenario_s19_langsmith_trace()
            outcomes["S19"] = "ok" if r.get("tool_result") else "no-tool_result"
        except RemoteNodeNotRegistered:
            outcomes["S19"] = "SKIP: llm_tool node not running"
            logger.warning("S19: llm_tool node not registered — is a2a-llm-tool running?")
        except Exception as exc:
            logger.error("S19 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S19"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S20 — cache hit (Sprint 7 Case 22)
        try:
            r = await scenario_s20_cache_hit()
            outcomes["S20"] = "ok (cache hit)" if r.get("cache_hit") else f"MISS: result1={r.get('cached_result_1')!r} result2={r.get('cached_result_2')!r}"
        except RemoteNodeNotRegistered:
            outcomes["S20"] = "SKIP: cached_node not running"
            logger.warning("S20: cached_node not registered — is a2a-cached running?")
        except Exception as exc:
            logger.error("S20 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S20"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S21 — TTL expiry (Sprint 7)
        try:
            r = await scenario_s21_ttl_expiry()
            outcomes["S21"] = "ok (TTL expired)" if r.get("expiry_verified") else f"NOT EXPIRED: result1={r.get('cached_result_1')!r} result3={r.get('cached_result_3')!r}"
        except RemoteNodeNotRegistered:
            outcomes["S21"] = "SKIP: cached_node not running"
            logger.warning("S21: cached_node not registered — is a2a-cached running?")
        except Exception as exc:
            logger.error("S21 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S21"] = f"FAILED: {type(exc).__name__}: {exc}"

        # S22 — key scoping (Sprint 7)
        try:
            r = await scenario_s22_key_scoping()
            ok = r.get("diff_inputs") and r.get("same_repeat")
            outcomes["S22"] = "ok (scoping verified)" if ok else f"UNEXPECTED diff_inputs={r.get('diff_inputs')} same_repeat={r.get('same_repeat')}"
        except RemoteNodeNotRegistered:
            outcomes["S22"] = "SKIP: cached_node not running"
            logger.warning("S22: cached_node not registered — is a2a-cached running?")
        except Exception as exc:
            logger.error("S22 failed [%s]: %s", type(exc).__name__, exc)
            outcomes["S22"] = f"FAILED: {type(exc).__name__}: {exc}"

        # Summary
        logger.info("═══ Sprint 7 demo summary ═══")
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
