from __future__ import annotations

import logging

from sdk.amaze import remote_node, serve_node

logger = logging.getLogger(__name__)

GRAPH_ID = "demo_graph_v1"


@remote_node(graph_id=GRAPH_ID, node_name="counter")
async def counter_handler(state: dict, config: dict) -> dict:
    count = state.get("count", 0)
    langgraph_step = config.get("metadata", {}).get("langgraph_step", 0)
    logger.info("counter_node: count=%d langgraph_step=%d", count, langgraph_step)
    return {
        "count": count + 1,
        "langgraph_step_echo": langgraph_step,
        "log_trail": [f"counter_node: count={count + 1} step={langgraph_step}"],
    }


if __name__ == "__main__":
    serve_node()
