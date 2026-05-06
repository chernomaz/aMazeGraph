from __future__ import annotations

import logging

from sdk.amaze import remote_node, serve_node

logger = logging.getLogger(__name__)

GRAPH_ID = "demo_graph_v1"


@remote_node(graph_id=GRAPH_ID, node_name="schema_remote")
async def schema_handler(state: dict, config: dict) -> dict:
    user_request: str = state.get("user_request", "")
    private_data: str = state.get("private_data", "")

    logger.info(
        "schema_remote received user_request=%r private_data=%r",
        user_request[:60],
        private_data[:40],
    )

    final_answer = (
        f"schema_remote: answered '{user_request[:60]}' "
        f"using private='{private_data[:40]}'"
    )
    return {
        "final_answer": final_answer,
        "log_trail": ["schema_node: produced final_answer"],
    }


if __name__ == "__main__":
    serve_node()
