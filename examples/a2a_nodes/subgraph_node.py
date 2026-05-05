from __future__ import annotations

import logging
from typing import TypedDict

from langgraph.graph import END, StateGraph

from sdk.amaze import remote_node, serve_node

logger = logging.getLogger(__name__)

GRAPH_ID = "demo_graph_v1"


# ---------------------------------------------------------------------------
# Inner-graph state
# ---------------------------------------------------------------------------


class SubgraphState(TypedDict):
    user_request: str
    step_a_result: str
    step_b_result: str


# ---------------------------------------------------------------------------
# Inner-graph node functions
# ---------------------------------------------------------------------------


async def step_a(state: SubgraphState) -> dict:
    req = state.get("user_request", "")
    result = f"step_a: processed {req}"
    logger.debug("step_a result=%r", result)
    return {"step_a_result": result}


async def step_b(state: SubgraphState) -> dict:
    a_result = state.get("step_a_result", "")
    result = f"step_b: extended {a_result}"
    logger.debug("step_b result=%r", result)
    return {"step_b_result": result}


# ---------------------------------------------------------------------------
# Remote handler — Sprint 3 cases #19 (subgraph as opaque node) and
# #20 (call subgraph manually)
# ---------------------------------------------------------------------------


@remote_node(graph_id=GRAPH_ID, node_name="subgraph")
async def subgraph_handler(state: dict, config: dict) -> dict:
    user_request = state.get("user_request", "")
    logger.info("subgraph_handler start user_request=%r", user_request[:60])

    # Build and compile the inner 2-step graph on each invocation so the
    # handler stays stateless (cheap for a demo; callers can cache if needed).
    builder: StateGraph = StateGraph(SubgraphState)
    builder.add_node("step_a", step_a)
    builder.add_node("step_b", step_b)
    builder.set_entry_point("step_a")
    builder.add_edge("step_a", "step_b")
    builder.add_edge("step_b", END)
    app = builder.compile()

    inner_result: SubgraphState = await app.ainvoke(
        {"user_request": user_request}
    )

    a_val = inner_result.get("step_a_result", "")
    b_val = inner_result.get("step_b_result", "")
    research_result = f"subgraph: step_a={a_val} step_b={b_val}"

    logger.info("subgraph_handler done research_result=%r", research_result)

    return {
        "research_result": research_result,
        "log_trail": [
            f"subgraph_node: ran 2-step subgraph for '{user_request[:60]}'"
        ],
    }


if __name__ == "__main__":
    serve_node()
