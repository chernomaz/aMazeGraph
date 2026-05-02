from __future__ import annotations

import asyncio
import logging

from examples.a2a_nodes._common import remote_node, serve_node

logger = logging.getLogger(__name__)

GRAPH_ID = "demo_graph_v1"


@remote_node(graph_id=GRAPH_ID, node_name="research_b")
async def research_b_handler(state: dict, config: dict) -> dict:
    """Fan-out branch B — simulates async remote work, returns its shard."""
    user_request = state.get("user_request", "")
    # Simulate some async work
    await asyncio.sleep(0)
    text = f"research_b: performance analysis perspective on '{user_request[:60]}'"
    logger.info("research_b_handler: returning result")
    return {"results": [text]}


if __name__ == "__main__":
    serve_node()
