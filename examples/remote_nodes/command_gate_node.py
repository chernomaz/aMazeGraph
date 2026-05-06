from __future__ import annotations

import json
import logging
import operator
from datetime import datetime
from typing import Annotated, Any, Literal, TypedDict

from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages
from langgraph.types import Command

from sdk.amaze import remote_node, serve_node

GRAPH_ID = "advanced_demo"

logger = logging.getLogger(__name__)


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


@remote_node(graph_id=GRAPH_ID, node_name="command_gate")
async def command_gate(state: AgentState) -> Command:
    node = "command_gate"

    logger.info("[%s] ENTER state=%s", node, compact(state))

    warnings = state.get("warnings", [])
    branch_results = state.get("branch_results", [])

    needs_review = any(w != "no major warning" for w in warnings)

    goto: Literal["final_answer", "human_review"] = (
        "human_review" if needs_review else "final_answer"
    )

    update = {
        "needs_review": needs_review,
        "logs": [f"{now()} | command_gate | needs_review={needs_review} | goto={goto}"],
    }

    logger.info("[%s] branch_count=%s warnings=%s goto=%s", node, len(branch_results), warnings, goto)
    logger.info("[%s] EXIT Command update=%s goto=%s", node, compact(update), goto)

    return Command(update=update, goto=goto)


if __name__ == "__main__":
    serve_node()
