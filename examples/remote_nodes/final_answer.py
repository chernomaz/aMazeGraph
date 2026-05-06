from __future__ import annotations

import json
import logging
import operator
from datetime import datetime
from typing import Annotated, Any, Literal, TypedDict
import os
from langchain_core.messages import AnyMessage, HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.graph.message import add_messages
from dotenv import load_dotenv
from sdk.amaze import remote_node, serve_node

GRAPH_ID = "advanced_demo"
load_dotenv()
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
    result = response.content if isinstance(response.content, str) else json.dumps(response.content)
    logger.info("[%s] LLM call finished. chars=%s", node_name, len(result))
    return result


log = logger


@remote_node(graph_id=GRAPH_ID, node_name="final_answer")
async def final_answer(state: AgentState) -> AgentState:
    node = "final_answer"

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
        system="You synthesize multiple expert branches into one practical final answer.",
        user=f"""
User request:
{state["user_request"]}

Branch results:
{joined}

Write a final answer that is concise, technical, and actionable.
""",
    )

    update: AgentState = {
        "final_answer": answer,
        "logs": [f"{now()} | final_answer | completed"],
        "messages": [SystemMessage(content=f"Final answer:\n{answer}")],
    }

    log.info("[%s] EXIT update=%s", node, compact(update))
    return update


if __name__ == "__main__":
    serve_node()
