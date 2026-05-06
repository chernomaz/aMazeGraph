from __future__ import annotations

import json
import re
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Path, Request
from pydantic import BaseModel, Field, field_validator
from redis import asyncio as aioredis
from redis.exceptions import RedisError

GRAPH_ID_PATTERN = r"^[a-z0-9_-]+$"
RUN_ID_PATTERN = r"^[A-Za-z0-9_\-]+$"
_GRAPH_ID_RE = re.compile(GRAPH_ID_PATTERN)

router = APIRouter()

_RUN_END_PROMOTE_LUA = """
local meta_key = KEYS[1]
local events_key = KEYS[2]
local force_failed = ARGV[1]
local raw = redis.call('GET', meta_key)
if not raw then
  return nil
end
local meta = cjson.decode(raw)
local new_status = 'done'
if force_failed == '1' then
  new_status = 'failed'
else
  local entries = redis.call('XRANGE', events_key, '-', '+')
  for _, entry in ipairs(entries) do
    local fields = entry[2]
    for i = 1, #fields, 2 do
      if fields[i] == 'event' and fields[i+1] == 'node-error' then
        new_status = 'failed'
        break
      end
    end
    if new_status == 'failed' then break end
  end
end
meta.status = new_status
redis.call('SET', meta_key, cjson.encode(meta))
return new_status
"""


class RunEventRequest(BaseModel):
    event: Literal["run-start", "node-enter", "node-exit", "node-error", "run-end"]
    graph_id: str = Field(min_length=1, max_length=128)
    node_name: str | None = Field(default=None, max_length=64)
    trace_id: str | None = None
    status: str | None = None
    error: str | None = None
    error_kind: str | None = None
    ts: str

    @field_validator("graph_id")
    @classmethod
    def _validate_graph_id(cls, v: str) -> str:
        if not _GRAPH_ID_RE.match(v):
            raise ValueError("graph_id must match ^[a-z0-9_-]+$")
        return v


class RunEventResponse(BaseModel):
    status: Literal["ok"] = "ok"
    stream_id: str


class RunMeta(BaseModel):
    graph_id: str
    trace_id: str | None = None
    started_at: str
    status: Literal["running", "done", "failed"]


class RunResponse(BaseModel):
    meta: RunMeta
    events: list[dict[str, Any]]


def _run_meta_key(run_id: str) -> str:
    return f"run:{run_id}:meta"


def _run_events_key(run_id: str) -> str:
    return f"run:{run_id}:events"


def _redis(request: Request) -> aioredis.Redis:
    return request.app.state.redis


@router.post("/runs/{run_id}/events", response_model=RunEventResponse)
async def append_run_event(
    req: RunEventRequest,
    request: Request,
    run_id: str = Path(pattern=RUN_ID_PATTERN, max_length=128),
) -> RunEventResponse:
    redis_client = _redis(request)
    fields: dict[str, str] = {
        "event": req.event,
        "graph_id": req.graph_id,
        "node_name": req.node_name or "",
        "trace_id": req.trace_id or "",
        "status": req.status or "",
        "error": req.error or "",
        "ts": req.ts,
    }
    if req.error_kind:
        fields["error_kind"] = req.error_kind
    meta_key = _run_meta_key(run_id)
    events_key = _run_events_key(run_id)
    try:
        meta_payload = json.dumps(
            {
                "graph_id": req.graph_id,
                "trace_id": req.trace_id,
                "started_at": req.ts,
                "status": "running",
            }
        )
        await redis_client.set(meta_key, meta_payload, nx=True)
        stream_id = await redis_client.xadd(events_key, fields)
        if req.event == "run-end":
            force_failed = "1" if (req.error or req.status == "failed") else "0"
            await redis_client.eval(
                _RUN_END_PROMOTE_LUA,
                2,
                meta_key,
                events_key,
                force_failed,
            )
    except RedisError:
        raise HTTPException(status_code=503, detail="redis-error")
    return RunEventResponse(stream_id=stream_id)


@router.get("/runs/{run_id}", response_model=RunResponse)
async def get_run(
    request: Request,
    run_id: str = Path(pattern=RUN_ID_PATTERN, max_length=128),
) -> RunResponse:
    redis_client = _redis(request)
    try:
        raw_meta = await redis_client.get(_run_meta_key(run_id))
        if raw_meta is None:
            raise HTTPException(
                status_code=404,
                detail={"detail": "run-not-found", "run_id": run_id},
            )
        entries = await redis_client.xrange(_run_events_key(run_id), "-", "+")
    except RedisError:
        raise HTTPException(status_code=503, detail="redis-error")

    meta_data = json.loads(raw_meta)
    events: list[dict[str, Any]] = []
    for stream_id, fields in entries:
        item: dict[str, Any] = {"id": stream_id}
        item.update(fields)
        events.append(item)
    return RunResponse(meta=RunMeta(**meta_data), events=events)
