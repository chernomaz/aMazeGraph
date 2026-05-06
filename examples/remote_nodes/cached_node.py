from __future__ import annotations

import logging
import time

from sdk.amaze import remote_node, serve_node

logger = logging.getLogger(__name__)


@remote_node(graph_id="demo_graph_v1", node_name="cached_node", cache_ttl=2)
async def cached_handler(state: dict, config: dict) -> dict:
    input_val = state.get("input", "")
    ts = f"{time.time():.3f}"
    logger.info("cached_node: executing (not a cache hit) input=%r ts=%s", input_val, ts)
    return {
        "cached_result": f"result-for-{input_val}-at-{ts}",
    }


if __name__ == "__main__":
    serve_node()
