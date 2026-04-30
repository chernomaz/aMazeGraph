from __future__ import annotations

import os

from examples.a2a_nodes._common import build_node_app, serve, setup_logging

GRAPH_ID = "demo_graph_v1"
NODE_NAME = "research"


async def research_handler(state: dict, config: dict) -> dict:
    user_request = state.get("user_request", "")
    text = (
        f"Research summary for: {user_request[:120]}. "
        "Remote LangGraph executes node bodies on separate hosts via the orchestrator."
    )
    return {"research_result": text}


def main() -> None:
    setup_logging(f"a2a-{NODE_NAME}")
    host = os.environ.get("RESEARCH_NODE_HOST", "0.0.0.0")
    port = int(os.environ.get("RESEARCH_NODE_PORT", "9002"))
    public_endpoint = os.environ.get(
        "RESEARCH_NODE_PUBLIC_ENDPOINT",
        f"http://localhost:{port}/invoke",
    )
    orchestrator_url = os.environ.get(
        "AMAZE_ORCHESTRATOR_URL", "http://localhost:8001"
    )
    app = build_node_app(
        graph_id=GRAPH_ID,
        node_name=NODE_NAME,
        handler=research_handler,
        orchestrator_url=orchestrator_url,
        public_endpoint=public_endpoint,
    )
    serve(app, host, port)


if __name__ == "__main__":
    main()
