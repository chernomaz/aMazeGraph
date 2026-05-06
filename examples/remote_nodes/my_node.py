"""
Template for a custom remote node — copy, rename, and modify.

Run without Docker:
    export AMAZE_NODE_PORT=9020
    export AMAZE_NODE_PUBLIC_ENDPOINT=http://localhost:9020/invoke
    export AMAZE_ORCHESTRATOR_URL=http://localhost:8011
    python -m examples.remote_nodes.my_node
"""
from __future__ import annotations

import logging

from sdk.amaze import remote_node, serve_node

GRAPH_ID = "my_graph"

logger = logging.getLogger(__name__)


@remote_node(graph_id=GRAPH_ID, node_name="my_node")
async def my_node_handler(state: dict, config: dict) -> dict:
    logger.info("my_node received state keys=%s", list(state.keys()))
    # TODO: replace with your logic
    result = f"processed: {state.get('input', '')}"
    return {"output": result}


if __name__ == "__main__":
    serve_node()
