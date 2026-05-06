from __future__ import annotations

from langgraph.types import Command

from sdk.amaze import remote_node, serve_node, setup_logging

GRAPH_ID = "demo_graph_v1"


@remote_node(graph_id=GRAPH_ID, node_name="command")
async def command_handler(state: dict, config: dict) -> Command:
    mode = state.get("mode", "single")
    if mode == "update_goto":
        return Command(
            update={"cmd_result": f"processed:{state.get('user_request', '')[:40]}"},
            goto="cmd_sink",
        )
    if mode == "multi":
        return Command(update={}, goto=["cmd_sink_a", "writer"])
    if mode == "bad_goto":
        return Command(update={}, goto="nonexistent_node_xyz")
    # default: "single"
    return Command(update={"cmd_result": "single-goto-result"}, goto="cmd_sink")


if __name__ == "__main__":
    setup_logging("remote-command")
    serve_node()
