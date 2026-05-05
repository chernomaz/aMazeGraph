from __future__ import annotations

import logging

from examples.a2a_nodes._common import remote_node, serve_node

GRAPH_ID = "demo_graph_v1"

logger = logging.getLogger(__name__)


@remote_node(graph_id=GRAPH_ID, node_name="research")
async def research_handler(state: dict, config: dict) -> dict:
    logger.info("research received state keys=%s", list(state.keys()))
    logger.info("research received user_request=%r", state.get("user_request", "")[:120])
    user_request = state.get("user_request", "")
    text = (
        f"Research summary for: {user_request[:120]}. "
        "Remote LangGraph executes node bodies on separate hosts via the orchestrator."
    )
    return {
        "research_result": text,
        "log_trail": [f"research_node: produced {len(text)} chars"],
    }


if __name__ == "__main__":
    serve_node()
