from __future__ import annotations

import logging
import os
from typing import Any

from sdk.amaze import remote_node, serve_node

try:
    from langchain_openai import ChatOpenAI
    from langchain_mcp_adapters.client import MultiServerMCPClient

    _LANGCHAIN_AVAILABLE = True
except ImportError:
    _LANGCHAIN_AVAILABLE = False

logger = logging.getLogger(__name__)

_SKIP_NO_KEY = {
    "results": ["[skipped: no OPENAI_API_KEY]"],
    "tool_result": "[skipped: no OPENAI_API_KEY]",
}


@remote_node(graph_id="demo_graph_v1", node_name="llm_tool")
async def llm_tool_handler(state: dict, config: dict) -> dict:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        return _SKIP_NO_KEY

    if not _LANGCHAIN_AVAILABLE:
        logger.error(
            "langchain_openai / langchain_mcp_adapters not installed; "
            "cannot execute llm_tool node"
        )
        return _SKIP_NO_KEY

    mcp_url = os.environ.get("MCP_URL", "http://mcp:8000/mcp")
    prompt = (
        f"Use the web_search tool to research: "
        f"{state.get('user_request', 'what is LangGraph?')}"
    )

    model = ChatOpenAI(model="gpt-4o-mini", api_key=api_key)

    tools: list[Any] = []
    mcp_available = True

    try:
        async with MultiServerMCPClient(
            {"mcp": {"url": mcp_url, "transport": "streamable_http"}}
        ) as mcp_client:
            tools = mcp_client.get_tools()
            llm_with_tools = model.bind_tools(tools)

            first_response = await llm_with_tools.ainvoke(
                [{"role": "human", "content": prompt}]
            )

            tool_result = ""
            final_response_content: str

            if first_response.tool_calls:
                tool_results_messages: list[dict] = [
                    {"role": "human", "content": prompt},
                    first_response,
                ]

                for tc in first_response.tool_calls:
                    tool_name = tc["name"]
                    tool_args = tc["args"]
                    tool_id = tc.get("id", tool_name)

                    matched_tool = next(
                        (t for t in tools if t.name == tool_name), None
                    )
                    if matched_tool is not None:
                        raw_result = await matched_tool.ainvoke(tool_args)
                        result_content = (
                            raw_result
                            if isinstance(raw_result, str)
                            else str(raw_result)
                        )
                    else:
                        result_content = f"[tool {tool_name!r} not found]"

                    tool_result += result_content
                    tool_results_messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": tool_id,
                            "content": result_content,
                        }
                    )

                final_response = await llm_with_tools.ainvoke(tool_results_messages)
                final_response_content = final_response.content or ""
            else:
                final_response_content = first_response.content or ""

    except Exception as exc:  # noqa: BLE001
        logger.warning("MCP connection failed (%s); calling model without tools", exc)
        mcp_available = False
        tool_result = "[mcp_unavailable]"

        plain_response = await model.ainvoke(
            [{"role": "human", "content": prompt}]
        )
        final_response_content = plain_response.content or ""

    return {
        "messages": [
            {"role": "assistant", "content": final_response_content},
        ],
        "results": [final_response_content],
        "tool_result": tool_result if mcp_available else "[mcp_unavailable]",
    }


if __name__ == "__main__":
    serve_node()
