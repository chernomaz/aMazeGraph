from __future__ import annotations

import logging

from langgraph.types import Command, Send

from examples.a2a_nodes._common import remote_node, serve_node, setup_logging

GRAPH_ID = "demo_graph_v1"

logger = logging.getLogger(__name__)


@remote_node(graph_id=GRAPH_ID, node_name="send_dispatcher")
async def send_handler(state: dict, config: dict) -> Command:
    mode = state.get("mode", "single")
    logger.info("send_dispatcher mode=%r input=%r", mode, state.get("input", "")[:80])
    if mode == "parallel":
        return Command(goto=[
            Send("send_sink_a", {"branch": "a", "val": state.get("input", "")}),
            Send("send_sink_b", {"branch": "b", "val": state.get("input", "")}),
        ])
    if mode == "with_update":
        return Command(
            update={"status": "dispatched"},
            goto=[Send("send_sink", {"val": state.get("input", "")})],
        )
    if mode == "bare_send":
        return Send("send_sink", {"val": state.get("input", "")})
    return Command(goto=[Send("send_sink", {"val": state.get("input", "")})])


if __name__ == "__main__":
    setup_logging("a2a-send")
    serve_node()
