from __future__ import annotations

import logging

from examples.a2a_nodes._common import remote_node, serve_node, setup_logging

GRAPH_ID = "demo_graph_v1"

logger = logging.getLogger(__name__)


@remote_node(graph_id=GRAPH_ID, node_name="planner")
async def planner_handler(state: dict, config: dict) -> dict:
    topic = state.get("topic", "distributed systems")
    logger.info("planner received topic=%r", topic)

    research_query = f"In-depth technical analysis of: {topic}"
    writing_brief = f"Write a concise engineering overview of: {topic}"
    research_notes = (
        f"Focus on how {topic} handles concurrency, failure modes, and scalability. "
        "Include concrete trade-offs."
    )

    logger.info(
        "planner produced research_query=%r writing_brief=%r",
        research_query[:60],
        writing_brief[:60],
    )
    return {
        "research_query": research_query,
        "writing_brief": writing_brief,
        "research_notes": research_notes,
    }


if __name__ == "__main__":
    setup_logging("a2a-planner")
    serve_node()
