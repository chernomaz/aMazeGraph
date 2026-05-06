from __future__ import annotations

import logging
import os
import sys
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis import asyncio as aioredis
from redis.exceptions import RedisError

from services.orchestrator.cache import router as cache_router
from services.orchestrator.events import router as events_router
from services.orchestrator.graph_manifest import router as graph_manifest_router
from services.orchestrator.registry import router as registry_router

_REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379")

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


class _HealthcheckFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return "GET /healthz" not in msg and "GET /health " not in msg


def setup_logging(service_name: str) -> None:
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


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    redis_client = aioredis.from_url(_REDIS_URL, decode_responses=True)
    _app.state.redis = redis_client
    try:
        yield
    finally:
        await redis_client.aclose()


app = FastAPI(title="aMazeGraph Orchestrator", lifespan=lifespan)

if _otel_enabled:
    FastAPIInstrumentor.instrument_app(app)

app.include_router(registry_router)
app.include_router(graph_manifest_router)
app.include_router(events_router)
app.include_router(cache_router)


@app.get("/health")
async def health(request: Request):
    try:
        await request.app.state.redis.ping()
    except RedisError:
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "redis": "down"},
        )
    return {"status": "ok", "redis": "ok"}
