# file: langgraph_advanced_demo.py

import os
import json
import logging
import operator
from datetime import datetime
from typing import Annotated, Literal, TypedDict, Any

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage, AnyMessage

#from langgraph.graph import StateGraph, START, END
from langgraph.graph.message import add_messages
from langgraph.types import Command
from sdk.amaze import AmazeGraph
from langgraph.graph import START, END
from dotenv import load_dotenv
load_dotenv()
# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)

log = logging.getLogger("advanced-langgraph-demo")


def now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def compact(value: Any, max_len: int = 700) -> str:
    text = json.dumps(value, ensure_ascii=False, default=str)
    return text if len(text) <= max_len else text[:max_len] + "...<truncated>"


def text_of(response: Any) -> str:
    """
    LangChain chat responses usually expose .content.
    Keep this defensive because provider content may sometimes be structured.
    """
    content = getattr(response, "content", response)
    if isinstance(content, str):
        return content
    return json.dumps(content, ensure_ascii=False, default=str)


# ---------------------------------------------------------------------
# LangSmith tracing
# ---------------------------------------------------------------------
# LangSmith tracing is activated by env vars:
from dotenv import load_dotenv


# LangChain docs show OpenAI access through langchain-openai and tracing
# through LangSmith env vars. OpenAI needs OPENAI_API_KEY.
# ---------------------------------------------------------------------


llm = ChatOpenAI(
    model="gpt-4.1-mini",
    temperature=0,
)
def now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

# ---------------------------------------------------------------------
# State
# ---------------------------------------------------------------------
# Reducers:
# - messages uses add_messages
# - logs, branch_results, warnings use operator.add
#
# This matters because parallel branches can update the same state keys.
# Without reducers, parallel updates to the same key may conflict or overwrite.
# ---------------------------------------------------------------------

class AgentState(TypedDict, total=False):
    user_request: str

    # Chat history / LLM trace-friendly messages.
    messages: Annotated[list[AnyMessage], add_messages]

    # Append-only logs from all nodes.
    logs: Annotated[list[str], operator.add]

    # Parallel branch outputs.
    branch_results: Annotated[list[dict[str, str]], operator.add]

    # Warnings produced by risk/compliance branches.
    warnings: Annotated[list[str], operator.add]

    # Normal overwritten fields.
    route: Literal["direct", "deep"]
    final_answer: str
    needs_review: bool


# ---------------------------------------------------------------------
# LLM helper
# ---------------------------------------------------------------------

def call_llm(node_name: str, system: str, user: str) -> str:
    log.info("[%s] LLM call started", node_name)
    log.info("[%s] system=%s", node_name, system)
    log.info("[%s] user=%s", node_name, user)

    response = llm.invoke(
        [
            SystemMessage(content=system),
            HumanMessage(content=user),
        ],
        config={
            "run_name": node_name,
            "tags": ["langgraph-demo", node_name],
            "metadata": {
                "node": node_name,
                "demo": "reducers-fanout-command",
            },
        },
    )

    result = text_of(response)
    log.info("[%s] LLM call finished. chars=%s", node_name, len(result))
    log.info("[%s] LLM result=%s", node_name, result[:1000])
    return result


# ---------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------
#
# def classify(state: AgentState) -> AgentState:
#     node = "classify"
#     request = state["user_request"]
#
#     log.info("[%s] ENTER state=%s", node, compact(state))
#
#     prompt = f"""
# Classify this user request.
#
# Return only one word:
# - direct: simple question, can answer directly
# - deep: needs multiple parallel analysis branches
#
# User request:
# {request}
# """
#
#     raw = call_llm(
#         node,
#         system="You are a strict router. Output only: direct or deep.",
#         user=prompt,
#     ).strip().lower()
#
#     route: Literal["direct", "deep"] = "deep" if "deep" in raw else "direct"
#
#     update: AgentState = {
#         "route": route,
#         "logs": [f"{now()} | classify | route={route} | raw={raw!r}"],
#         "messages": [
#             HumanMessage(content=request),
#             SystemMessage(content=f"Router selected: {route}"),
#         ],
#     }
#
#     log.info("[%s] EXIT update=%s", node, compact(update))
#     return update


def route_after_classify(state: AgentState) -> Literal["direct_answer", "fanout_start"]:
    route = state.get("route", "deep")
    next_node = "direct_answer" if route == "direct" else "fanout_start"

    log.info(
        "[route_after_classify] route=%s -> next=%s",
        route,
        next_node,
    )

    return next_node


def direct_answer(state: AgentState) -> AgentState:
    node = "direct_answer"
    request = state["user_request"]

    log.info("[%s] ENTER state=%s", node, compact(state))

    answer = call_llm(
        node,
        system="Answer clearly and concisely.",
        user=request,
    )

    update: AgentState = {
        "final_answer": answer,
        "logs": [f"{now()} | direct_answer | completed"],
        "messages": [SystemMessage(content=f"Direct answer:\n{answer}")],
    }

    log.info("[%s] EXIT update=%s", node, compact(update))
    return update


# def fanout_start(state: AgentState) -> AgentState:
#     node = "fanout_start"
#
#     log.info("[%s] ENTER state=%s", node, compact(state))
#     log.info(
#         "[%s] This node fans out to: architecture_branch, risk_branch, implementation_branch",
#         node,
#     )
#
#     return {
#         "logs": [f"{now()} | fanout_start | parallel fan-out started"],
#     }


# def architecture_branch(state: AgentState) -> AgentState:
#     node = "architecture_branch"
#     request = state["user_request"]
#
#     log.info("[%s] ENTER state=%s", node, compact(state))
#
#     result = call_llm(
#         node,
#         system="You are a senior distributed-systems architect.",
#         user=f"""
# Analyze the architecture implications of this request.
# Focus on graph structure, orchestration, state, and distributed execution.
#
# Request:
# {request}
# """,
#     )
#
#     update: AgentState = {
#         "branch_results": [
#             {
#                 "branch": "architecture",
#                 "content": result,
#             }
#         ],
#         "logs": [f"{now()} | architecture_branch | completed"],
#     }
#
#     log.info("[%s] EXIT update=%s", node, compact(update))
#     return update


def risk_branch(state: AgentState) -> AgentState:
    node = "risk_branch"
    request = state["user_request"]

    log.info("[%s] ENTER state=%s", node, compact(state))

    result = call_llm(
        node,
        system="You are a security and reliability reviewer.",
        user=f"""
Review this request for risks, missing controls, failure modes, and production concerns.

Request:
{request}
""",
    )

    warning = "manual review recommended" if any(
        word in result.lower()
        for word in ["risk", "security", "unsafe", "production", "failure", "privacy"]
    ) else "no major warning"

    update: AgentState = {
        "branch_results": [
            {
                "branch": "risk",
                "content": result,
            }
        ],
        "warnings": [warning],
        "logs": [f"{now()} | risk_branch | completed | warning={warning}"],
    }

    log.info("[%s] EXIT update=%s", node, compact(update))
    return update


def implementation_branch(state: AgentState) -> AgentState:
    node = "implementation_branch"
    request = state["user_request"]

    log.info("[%s] ENTER state=%s", node, compact(state))

    result = call_llm(
        node,
        system="You are a Python/LangGraph implementation expert.",
        user=f"""
Create concrete implementation guidance for this request.
Focus on LangGraph APIs, reducers, edges, conditional routing, and Command.

Request:
{request}
""",
    )

    update: AgentState = {
        "branch_results": [
            {
                "branch": "implementation",
                "content": result,
            }
        ],
        "logs": [f"{now()} | implementation_branch | completed"],
    }

    log.info("[%s] EXIT update=%s", node, compact(update))
    return update


# ---------------------------------------------------------------------
# Command node
# ---------------------------------------------------------------------
# This node both:
# 1. updates state
# 2. decides where to go next
#
# IMPORTANT:
# Do not add normal static edges from this node.
# It routes only through Command(goto=...).
# ---------------------------------------------------------------------

# def command_gate(
#     state: AgentState,
# ) -> Command[Literal["final_answer", "human_review"]]:
#     node = "command_gate"
#
#     log.info("[%s] ENTER state=%s", node, compact(state))
#
#     warnings = state.get("warnings", [])
#     branch_results = state.get("branch_results", [])
#
#     needs_review = any(w != "no major warning" for w in warnings)
#
#     goto: Literal["final_answer", "human_review"] = (
#         "human_review" if needs_review else "final_answer"
#     )
#
#     update = {
#         "needs_review": needs_review,
#         "logs": [
#             f"{now()} | command_gate | needs_review={needs_review} | goto={goto}"
#         ],
#     }
#
#     log.info(
#         "[%s] branch_count=%s warnings=%s goto=%s",
#         node,
#         len(branch_results),
#         warnings,
#         goto,
#     )
#     log.info("[%s] EXIT Command update=%s goto=%s", node, compact(update), goto)
#
#     return Command(update=update, goto=goto)

#
# def final_answer(state: AgentState) -> AgentState:
#     node = "final_answer"
#
#     log.info("[%s] ENTER state=%s", node, compact(state))
#
#     ordered = sorted(
#         state.get("branch_results", []),
#         key=lambda x: x.get("branch", ""),
#     )
#
#     joined = "\n\n".join(
#         f"## {item['branch']}\n{item['content']}"
#         for item in ordered
#     )
#
#     answer = call_llm(
#         node,
#         system="You synthesize multiple expert branches into one practical final answer.",
#         user=f"""
# User request:
# {state["user_request"]}
#
# Branch results:
# {joined}
#
# Write a final answer that is concise, technical, and actionable.
# """,
#     )
#
#     update: AgentState = {
#         "final_answer": answer,
#         "logs": [f"{now()} | final_answer | completed"],
#         "messages": [SystemMessage(content=f"Final answer:\n{answer}")],
#     }
#
#     log.info("[%s] EXIT update=%s", node, compact(update))
#     return update


def human_review(state: AgentState) -> AgentState:
    node = "human_review"

    log.info("[%s] ENTER state=%s", node, compact(state))

    ordered = sorted(
        state.get("branch_results", []),
        key=lambda x: x.get("branch", ""),
    )

    joined = "\n\n".join(
        f"## {item['branch']}\n{item['content']}"
        for item in ordered
    )

    answer = call_llm(
        node,
        system="You prepare a cautious answer because risk review requested manual review.",
        user=f"""
User request:
{state["user_request"]}

Warnings:
{state.get("warnings", [])}

Branch results:
{joined}

Write a final answer, but explicitly mark which parts need human review before production use.
""",
    )

    update: AgentState = {
        "final_answer": answer,
        "logs": [f"{now()} | human_review | completed"],
        "messages": [SystemMessage(content=f"Human-review answer:\n{answer}")],
    }

    log.info("[%s] EXIT update=%s", node, compact(update))
    return update


# ---------------------------------------------------------------------
# Build graph
# ---------------------------------------------------------------------

def build_graph():
    #builder = StateGraph(AgentState)
    #builder = AmazeGraph(AgentState, orchestrator_url="http://localhost:8011")
    #builder = AmazeGraph(AgentState, graph_id="advanced_demo", orchestrator_url="http://localhost:8011")
    builder = AmazeGraph(AgentState, graph_id="advanced_demo", orchestrator_url="http://localhost:8011", sync=True)
    builder.remote_node("classify")  # no local function — resolved via orchestrator
    #builder.add_node("classify", classify)
    builder.add_node("direct_answer", direct_answer)
    builder.remote_node( "fanout_start")

    builder.remote_node( "architecture_branch")
    builder.add_node("risk_branch", risk_branch)
    builder.add_node("implementation_branch", implementation_branch)

    builder.remote_node("command_gate")
    builder.remote_node("final_answer")
    builder.add_node("human_review", human_review)

    builder.add_edge(START, "classify")

    # Conditional routing.
    builder.add_conditional_edges(
        "classify",
        route_after_classify,
        {
            "direct_answer": "direct_answer",
            "fanout_start": "fanout_start",
        },
    )

    builder.add_edge("direct_answer", END)

    # Parallel fan-out via multiple static edges.
    builder.add_edge("fanout_start", "architecture_branch")
    builder.add_edge("fanout_start", "risk_branch")
    builder.add_edge("fanout_start", "implementation_branch")

    # Fan-in.
    # command_gate waits until all incoming branches from the same superstep finish.
    builder.add_edge("architecture_branch", "command_gate")
    builder.add_edge("risk_branch", "command_gate")
    builder.add_edge("implementation_branch", "command_gate")

    # No static edge from command_gate.
    # command_gate returns Command(update=..., goto=...).

    builder.add_edge("final_answer", END)
    builder.add_edge("human_review", END)

    return builder.compile()


# ---------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------

if __name__ == "__main__":
    graph = build_graph()

    initial_state: AgentState = {
        "user_request": (
            "Design a LangGraph workflow for an AI control plane that can "
            "route tasks, run analysis in parallel, detect risk, and produce "
            "a final answer."
        ),
        "messages": [],
        "logs": [],
        "branch_results": [],
        "warnings": [],
    }

    config = {
        "configurable": {
            "thread_id": "demo-thread-001",
        },
        "run_name": "advanced-langgraph-reducers-fanout-command-demo",
        "tags": ["demo", "reducers", "fanout", "command", "openai"],
        "metadata": {
            "example": "reducers_parallel_conditional_command",
        },
        # Optional concurrency control.
        "max_concurrency": 3,
    }

    log.info("GRAPH RUN START")

    for step in graph.stream(
        initial_state,
        config=config,
        stream_mode="updates",
    ):
        log.info("STREAM STEP: %s", compact(step, max_len=1500))

    log.info("GRAPH RUN FINISHED")

    result = graph.invoke(initial_state, config=config)

    print("\n\n================ FINAL ANSWER ================\n")
    print(result.get("final_answer"))

    print("\n\n================ LOGS ================\n")
    for line in result.get("logs", []):
        print(line)

    print("\n\n================ WARNINGS ================\n")
    for warning in result.get("warnings", []):
        print(warning)