from __future__ import annotations

import os

from examples.a2a_nodes._common import build_node_app, serve, setup_logging

GRAPH_ID = "demo_graph_v1"
NODE_NAME = "writer"


async def writer_handler(state: dict, config: dict) -> dict:
    research_result = state.get("research_result", "")
    user_request = state.get("user_request", "")
    final = (
        f"Final answer for '{user_request[:80]}': {research_result} "
        "Architecture: driver -> orchestrator -> remote a2a nodes via JSON over HTTP."
    )
    return {"final_answer": final}


def main() -> None:
    setup_logging(f"a2a-{NODE_NAME}")
    host = os.environ.get("WRITER_NODE_HOST", "0.0.0.0")
    port = int(os.environ.get("WRITER_NODE_PORT", "9003"))
    public_endpoint = os.environ.get(
        "WRITER_NODE_PUBLIC_ENDPOINT",
        f"http://localhost:{port}/invoke",
    )
    orchestrator_url = os.environ.get(
        "AMAZE_ORCHESTRATOR_URL", "http://localhost:8001"
    )
    app = build_node_app(
        graph_id=GRAPH_ID,
        node_name=NODE_NAME,
        handler=writer_handler,
        orchestrator_url=orchestrator_url,
        public_endpoint=public_endpoint,
    )
    serve(app, host, port)


if __name__ == "__main__":
    main()
