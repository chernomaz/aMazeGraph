
from __future__ import annotations

from sdk.amaze import remote_node, serve_node


import logging
import operator
from datetime import datetime
from typing import Annotated, Literal, TypedDict, Any

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage, AnyMessage
from langgraph.graph.message import add_messages
import json
from dotenv import load_dotenv
logger = logging.getLogger(__name__)

GRAPH_ID = "advanced_demo"
load_dotenv()
llm = ChatOpenAI(model="gpt-4.1-mini", temperature=0)

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


class AgentState(TypedDict, total=False):
    user_request: str
    messages: Annotated[list[AnyMessage], add_messages]
    logs: Annotated[list[str], operator.add]
    branch_results: Annotated[list[dict[str, str]], operator.add]
    warnings: Annotated[list[str], operator.add]
    route: Literal["direct", "deep"]
    final_answer: str
    needs_review: bool


@remote_node(graph_id=GRAPH_ID, node_name="fanout_start")
async def  fanout_start(state: AgentState) -> AgentState:
    node = "fanout_start"

    log.info("[%s] ENTER state=%s", node, compact(state))
    log.info(
        "[%s] This node fans out to: architecture_branch, risk_branch, implementation_branch",
        node,
    )

    return {
        "logs": [f"{now()} | fanout_start | parallel fan-out started"],
    }

if __name__ == "__main__":
    serve_node()