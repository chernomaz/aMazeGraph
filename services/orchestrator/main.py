from __future__ import annotations

import json
import logging
import os
import re
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Literal

import uvicorn
from fastapi import FastAPI, HTTPException, Path, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from redis import asyncio as aioredis
from redis.exceptions import RedisError


class _HealthcheckFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return "GET /healthz" not in msg and "GET /health " not in msg


def _setup_logging(service_name: str) -> None:
    level = getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO)
    root = logging.getLogger()
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        handler = logging.StreamHandler(stream=sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                fmt=f"%(asctime)s [%(levelname)s] {service_name} %(name)s: %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
        root.addHandler(handler)
    root.setLevel(level)
    access = logging.getLogger("uvicorn.access")
    if not any(isinstance(f, _HealthcheckFilter) for f in access.filters):
        access.addFilter(_HealthcheckFilter())


_setup_logging("orchestrator")

REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")
ORCHESTRATOR_HOST = os.environ.get("ORCHESTRATOR_HOST", "0.0.0.0")
ORCHESTRATOR_PORT = int(os.environ.get("ORCHESTRATOR_PORT", "8001"))

GRAPH_ID_PATTERN = r"^[a-z0-9_-]+$"
NODE_NAME_PATTERN = r"^[A-Za-z0-9_\-]+$"
RUN_ID_PATTERN = r"^[A-Za-z0-9_\-]+$"
ENDPOINT_PATTERN = r"^https?://[^\s]+$"
ALLOWED_EVENTS = {"run-start", "node-enter", "node-exit", "node-error", "run-end"}

_GRAPH_ID_RE = re.compile(GRAPH_ID_PATTERN)
_ENDPOINT_RE = re.compile(ENDPOINT_PATTERN)


_otel_enabled = bool(os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"))
if _otel_enabled:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    _resource = Resource.create({"service.name": "orchestrator"})
    _provider = TracerProvider(resource=_resource)
    _exporter = OTLPSpanExporter(
        endpoint=os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"],
        insecure=True,
    )
    _provider.add_span_processor(BatchSpanProcessor(_exporter))
    trace.set_tracer_provider(_provider)


class RegisterNodeRequest(BaseModel):
    graph_id: str = Field(min_length=1, max_length=128)
    node_name: str = Field(min_length=1, max_length=64, pattern=NODE_NAME_PATTERN)
    endpoint: str = Field(min_length=1, max_length=2048)

    @field_validator("graph_id")
    @classmethod
    def _validate_graph_id(cls, v: str) -> str:
        if not _GRAPH_ID_RE.match(v):
            raise ValueError("graph_id must match ^[a-z0-9_-]+$")
        return v

    @field_validator("endpoint")
    @classmethod
    def _validate_endpoint(cls, v: str) -> str:
        if not _ENDPOINT_RE.match(v):
            raise ValueError("endpoint must be an absolute http(s) URL")
        return v


class RegisterNodeResponse(BaseModel):
    status: Literal["ok"] = "ok"
    graph_id: str
    node_name: str


class DeleteNodeResponse(BaseModel):
    status: Literal["ok"] = "ok"
    graph_id: str
    node_name: str


class ResolveNodeResponse(BaseModel):
    graph_id: str
    node_name: str
    endpoint: str


class RegisterGraphRequest(BaseModel):
    graph_id: str = Field(min_length=1, max_length=128)
    nodes: list[str]
    edges: list[tuple[str, str]]

    @field_validator("graph_id")
    @classmethod
    def _validate_graph_id(cls, v: str) -> str:
        if not _GRAPH_ID_RE.match(v):
            raise ValueError("graph_id must match ^[a-z0-9_-]+$")
        return v


class RegisterGraphResponse(BaseModel):
    status: Literal["ok"] = "ok"
    graph_id: str


class GraphManifestResponse(BaseModel):
    graph_id: str
    nodes: list[str]
    edges: list[tuple[str, str]]
    registered_at: str


class RunEventRequest(BaseModel):
    event: Literal["run-start", "node-enter", "node-exit", "node-error", "run-end"]
    graph_id: str = Field(min_length=1, max_length=128)
    node_name: str | None = Field(default=None, max_length=64)
    trace_id: str | None = None
    status: str | None = None
    error: str | None = None
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


class HealthResponse(BaseModel):
    status: Literal["ok"]
    redis: Literal["ok"]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    app.state.redis = redis_client
    try:
        yield
    finally:
        await redis_client.aclose()


app = FastAPI(title="aMazeGraph Orchestrator", lifespan=lifespan)

if _otel_enabled:
    FastAPIInstrumentor.instrument_app(app)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _node_key(graph_id: str, node_name: str) -> str:
    return f"graph_node:{graph_id}:{node_name}"


def _graph_key(graph_id: str) -> str:
    return f"graph:{graph_id}"


def _run_meta_key(run_id: str) -> str:
    return f"run:{run_id}:meta"


def _run_events_key(run_id: str) -> str:
    return f"run:{run_id}:events"


def _redis(request: Request) -> aioredis.Redis:
    return request.app.state.redis


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


@app.post("/register/node", response_model=RegisterNodeResponse)
async def register_node(req: RegisterNodeRequest, request: Request) -> RegisterNodeResponse:
    payload = json.dumps({"endpoint": req.endpoint, "registered_at": _now_iso()})
    try:
        await _redis(request).set(_node_key(req.graph_id, req.node_name), payload)
    except RedisError:
        raise HTTPException(status_code=503, detail="redis-error")
    return RegisterNodeResponse(graph_id=req.graph_id, node_name=req.node_name)


@app.delete("/register/node", response_model=DeleteNodeResponse)
async def unregister_node(req: RegisterNodeRequest, request: Request) -> DeleteNodeResponse:
    try:
        await _redis(request).delete(_node_key(req.graph_id, req.node_name))
    except RedisError:
        raise HTTPException(status_code=503, detail="redis-error")
    return DeleteNodeResponse(graph_id=req.graph_id, node_name=req.node_name)


@app.get("/resolve/node/{graph_id}/{node_name}", response_model=ResolveNodeResponse)
async def resolve_node(
    request: Request,
    graph_id: str = Path(pattern=GRAPH_ID_PATTERN, max_length=128),
    node_name: str = Path(pattern=NODE_NAME_PATTERN, max_length=64),
) -> ResolveNodeResponse:
    try:
        raw = await _redis(request).get(_node_key(graph_id, node_name))
    except RedisError:
        raise HTTPException(status_code=503, detail="redis-error")
    if raw is None:
        return JSONResponse(
            status_code=404,
            content={
                "detail": "node-not-registered",
                "graph_id": graph_id,
                "node_name": node_name,
            },
        )
    data = json.loads(raw)
    return ResolveNodeResponse(
        graph_id=graph_id,
        node_name=node_name,
        endpoint=data["endpoint"],
    )


@app.post("/register/graph", response_model=RegisterGraphResponse)
async def register_graph(req: RegisterGraphRequest, request: Request) -> RegisterGraphResponse:
    payload = json.dumps(
        {
            "nodes": req.nodes,
            "edges": [list(edge) for edge in req.edges],
            "registered_at": _now_iso(),
        }
    )
    try:
        await _redis(request).set(_graph_key(req.graph_id), payload)
    except RedisError:
        raise HTTPException(status_code=503, detail="redis-error")
    return RegisterGraphResponse(graph_id=req.graph_id)


@app.get("/graphs/{graph_id}", response_model=GraphManifestResponse)
async def get_graph(
    request: Request,
    graph_id: str = Path(pattern=GRAPH_ID_PATTERN, max_length=128),
) -> GraphManifestResponse:
    try:
        raw = await _redis(request).get(_graph_key(graph_id))
    except RedisError:
        raise HTTPException(status_code=503, detail="redis-error")
    if raw is None:
        raise HTTPException(
            status_code=404,
            detail={"detail": "graph-not-registered", "graph_id": graph_id},
        )
    data = json.loads(raw)
    return GraphManifestResponse(
        graph_id=graph_id,
        nodes=data["nodes"],
        edges=[tuple(edge) for edge in data["edges"]],
        registered_at=data["registered_at"],
    )


@app.post("/runs/{run_id}/events", response_model=RunEventResponse)
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


@app.get("/runs/{run_id}", response_model=RunResponse)
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


@app.get("/health")
async def health(request: Request):
    try:
        await _redis(request).ping()
    except RedisError:
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "redis": "down"},
        )
    return {"status": "ok", "redis": "ok"}


if __name__ == "__main__":
    uvicorn.run(
        app,
        host=ORCHESTRATOR_HOST,
        port=ORCHESTRATOR_PORT,
        log_config=None,
    )
