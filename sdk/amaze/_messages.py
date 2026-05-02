from __future__ import annotations

from typing import Any

from langchain_core.messages import (
    AIMessage,
    BaseMessage,
    HumanMessage,
    SystemMessage,
    ToolMessage,
)

_ROLE_BY_TYPE: dict[type[BaseMessage], str] = {
    HumanMessage: "human",
    AIMessage: "assistant",
    SystemMessage: "system",
    ToolMessage: "tool",
}


def _role_for(message: BaseMessage) -> str:
    for cls, role in _ROLE_BY_TYPE.items():
        if isinstance(message, cls):
            return role
    mtype = getattr(message, "type", None)
    if mtype == "human":
        return "human"
    if mtype in ("ai", "assistant"):
        return "assistant"
    if mtype == "system":
        return "system"
    if mtype == "tool":
        return "tool"
    raise ValueError(f"unsupported message class: {type(message).__name__}")


def serialize_messages(messages: list[BaseMessage]) -> list[dict]:
    out: list[dict] = []
    for m in messages:
        item: dict[str, Any] = {
            "role": _role_for(m),
            "content": m.content,
        }
        mid = getattr(m, "id", None)
        if mid is not None:
            item["id"] = mid

        if isinstance(m, AIMessage):
            tool_calls = getattr(m, "tool_calls", None)
            if tool_calls:
                # dict() ensures ToolCall TypedDict instances become plain dicts
                # that survive a json.loads round-trip on the receiving side.
                item["tool_calls"] = [dict(tc) for tc in tool_calls]

        if isinstance(m, ToolMessage):
            tcid = getattr(m, "tool_call_id", None)
            if tcid is not None:
                item["tool_call_id"] = tcid

        addl = getattr(m, "additional_kwargs", None)
        if addl:
            item["additional_kwargs"] = dict(addl)

        meta = getattr(m, "response_metadata", None)
        if meta:
            item["response_metadata"] = dict(meta)

        out.append(item)
    return out


def deserialize_messages(items: list[dict | BaseMessage]) -> list[BaseMessage]:
    out: list[BaseMessage] = []
    for item in items:
        if isinstance(item, BaseMessage):
            out.append(item)
            continue

        role = item.get("role")
        content = item.get("content", "")
        mid = item.get("id")
        addl = item.get("additional_kwargs") or {}
        meta = item.get("response_metadata") or {}

        kwargs: dict[str, Any] = {"content": content}
        if mid is not None:
            kwargs["id"] = mid
        if addl:
            kwargs["additional_kwargs"] = dict(addl)
        if meta:
            kwargs["response_metadata"] = dict(meta)

        if role in ("human", "user"):
            out.append(HumanMessage(**kwargs))
        elif role in ("assistant", "ai"):
            tool_calls = item.get("tool_calls")
            if tool_calls:
                kwargs["tool_calls"] = list(tool_calls)
            out.append(AIMessage(**kwargs))
        elif role == "system":
            out.append(SystemMessage(**kwargs))
        elif role == "tool":
            tcid = item.get("tool_call_id")
            if tcid is None:
                raise ValueError("tool message missing tool_call_id")
            kwargs["tool_call_id"] = tcid
            out.append(ToolMessage(**kwargs))
        else:
            raise ValueError(f"unknown message role: {role!r}")
    return out


__all__ = ["serialize_messages", "deserialize_messages"]
