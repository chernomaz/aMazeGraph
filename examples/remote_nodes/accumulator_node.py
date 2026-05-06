from __future__ import annotations

import logging

from sdk.amaze import remote_node, serve_node

logger = logging.getLogger(__name__)

GRAPH_ID = "demo_graph_v1"


@remote_node(graph_id=GRAPH_ID, node_name="accumulator")
async def accumulator_handler(state: dict, config: dict) -> dict:
    visits = state.get("visits", 0)
    input_val = state.get("input", "")
    logger.info("accumulator: visits=%d input=%s", visits + 1, input_val)
    return {
        "visits": visits + 1,
        "log": [input_val],
    }


if __name__ == "__main__":
    serve_node()
