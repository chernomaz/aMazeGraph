from __future__ import annotations

import logging

from sdk.amaze import remote_node, serve_node

logger = logging.getLogger(__name__)

GRAPH_ID = "demo_graph_v1"


@remote_node(graph_id=GRAPH_ID, node_name="audit")
async def audit_handler(state: dict, config: dict) -> dict:
    logger.info("audit_handler: state has %d keys", len(state))
    for key, val in state.items():
        logger.info("audit_handler: key=%s type=%s", key, type(val).__name__)
    return {}  # empty dict = no state changes


@remote_node(graph_id=GRAPH_ID, node_name="config_echo")
async def config_echo_handler(state: dict, config: dict, runtime) -> dict:
    # runtime is injected by serve_node when handler has 3 params
    thread_id = (config.get("configurable") or {}).get("thread_id", "")
    tenant_id = getattr(runtime.context, "tenant_id", "")
    logger.info("config_echo: thread_id=%s tenant_id=%s", thread_id, tenant_id)
    return {"echoed_thread": thread_id, "echoed_tenant": tenant_id}


if __name__ == "__main__":
    serve_node()
