from __future__ import annotations

import json
import logging
import operator
from datetime import datetime
from typing import Annotated, Any, Literal, TypedDict
import os
from dotenv import load_dotenv
from langchain_core.messages import AnyMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.graph.message import add_messages

from sdk.amaze import remote_node, serve_node
load_dotenv()
GRAPH_ID = "advanced_demo"
logger = logging.getLogger(__name__)

llm = ChatOpenAI(model="gpt-4.1-mini", temperature=0)


class AgentState(TypedDict, total=False):
    user_request: str
    messages: Annotated[list[AnyMessage], add_messages]
    logs: Annotated[list[str], operator.add]
    branch_results: Annotated[list[dict[str, str]], operator.add]
    warnings: Annotated[list[str], operator.add]
    route: Literal["direct", "deep"]
    final_answer: str
    needs_review: bool


def now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def compact(value: Any, max_len: int = 700) -> str:
    text = json.dumps(value, ensure_ascii=False, default=str)
    return text if len(text) <= max_len else text[:max_len] + "...<truncated>"


def call_llm(node_name: str, system: str, user: str) -> str:
    logger.info("[%s] LLM call started", node_name)
    response = llm.invoke([
        SystemMessage(content=system),
        HumanMessage(content=user),
    ] ,config={
            "run_name": node_name,
            "tags": ["langgraph-demo", node_name],
            "metadata": {
                "node": node_name,
                "demo": "reducers-fanout-command",
            },
        },
    )
    result = response.content if isinstance(response.content, str) else json.dumps(response.content)
    logger.info("[%s] LLM call finished. chars=%s", node_name, len(result))
    log.info("[%s] LLM result=%s", node_name, result[:1000])
    return result


log = logger


@remote_node(graph_id=GRAPH_ID, node_name="architecture_branch")
async def architecture_branch(state: AgentState) -> AgentState:
    node = "architecture_branch"
    request = state["user_request"]

    log.info("[%s] ENTER state=%s", node, compact(state))

    result = call_llm(
        node,
        system="You are a senior distributed-systems architect.",
        user=f"""
Analyze the architecture implications of this request.
Focus on graph structure, orchestration, state, and distributed execution.

Request:
{request}
""",
    )

    update: AgentState = {
        "branch_results": [
            {
                "branch": "architecture",
                "content": result,
            }
        ],
        "logs": [f"{now()} | architecture_branch | completed"],
    }

    log.info("[%s] EXIT update=%s", node, compact(update))
    return update


if __name__ == "__main__":
    serve_node()
