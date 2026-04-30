from __future__ import annotations

from examples.a2a_nodes._common import remote_node, serve_node

GRAPH_ID = "demo_graph_v1"


@remote_node(graph_id=GRAPH_ID, node_name="writer")
async def writer_handler(state: dict, config: dict) -> dict:
    research_result = state.get("research_result", "")
    user_request = state.get("user_request", "")
    final = (
        f"Final answer for '{user_request[:80]}': {research_result} "
        "Architecture: driver -> orchestrator -> remote a2a nodes via JSON over HTTP."
    )
    return {"final_answer": final}


if __name__ == "__main__":
    serve_node()
