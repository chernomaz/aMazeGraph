from __future__ import annotations

import logging
import operator
from datetime import datetime
from typing import Annotated, Literal, TypedDict

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage, AnyMessage
from langgraph.graph.message import add_messages

from sdk.amaze import remote_node, serve_node

GRAPH_ID = "advanced_demo"

logger = logging.getLogger(__name__)

import json
from dotenv import load_dotenv
logger = logging.getLogger(__name__)
GRAPH_ID = "advanced_demo"
load_dotenv()

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


@remote_node(graph_id=GRAPH_ID, node_name="classify")
async def classify_handler(state: AgentState, config: dict) -> AgentState:
    request = state["user_request"]

    response = llm.invoke([
        SystemMessage(content="You are a strict router. Output only: direct or deep."),
        HumanMessage(content=f"Classify this request:\n{request}"),
    ])
    raw = response.content.strip().lower()
    route = "deep" if "deep" in raw else "direct"

    return {
        "route": route,
        "logs": [f"{now()} | classify | route={route} | raw={raw!r}"],
        "messages": [
            HumanMessage(content=request),
            SystemMessage(content=f"Router selected: {route}"),
        ],
    }


if __name__ == "__main__":
    serve_node()
