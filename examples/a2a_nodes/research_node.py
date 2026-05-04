from __future__ import annotations

from examples.a2a_nodes._common import remote_node, serve_node

GRAPH_ID = "demo_graph_v1"


@remote_node(graph_id=GRAPH_ID, node_name="research")
async def research_handler(state: dict, config: dict) -> dict:
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
