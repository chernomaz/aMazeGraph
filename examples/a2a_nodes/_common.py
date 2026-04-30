from __future__ import annotations

import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Callable

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from sdk.amaze.langgraph import _init_otel

logger = logging.getLogger(__name__)

NodeHandler = Callable[[dict, dict], Awaitable[dict]]


class _HealthcheckFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return "GET /healthz" not in msg and "GET /health " not in msg


def setup_logging(service_name: str) -> None:
    level_name = os.environ.get("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
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


class InvokeRequest(BaseModel):
    graph_id: str
    node_name: str
    run_id: str | None = None
    trace_id: str | None = None
    state: dict[str, Any]
    config: dict[str, Any] = {}


class InvokeResponse(BaseModel):
    state_patch: dict[str, Any]


async def _register_with_backoff(
    orchestrator_url: str,
    body: dict,
    *,
    attempts: int = 30,
    delay: float = 2.0,
) -> None:
    last_exc: Exception | None = None
    async with httpx.AsyncClient(timeout=10.0) as client:
        for _ in range(attempts):
            try:
                r = await client.post(
                    f"{orchestrator_url}/register/node", json=body
                )
            except (httpx.ConnectError, httpx.ReadError) as exc:
                last_exc = exc
                await asyncio.sleep(delay)
                continue
            if 400 <= r.status_code < 500:
                raise RuntimeError(
                    f"register/node failed: status={r.status_code} body={r.text[:512]}"
                )
            if r.status_code // 100 == 2:
                return
            last_exc = RuntimeError(
                f"register/node 5xx: status={r.status_code} body={r.text[:512]}"
            )
            await asyncio.sleep(delay)
    raise RuntimeError(
        f"register/node exhausted retries: {last_exc}"
    )


async def _unregister_best_effort(orchestrator_url: str, body: dict) -> None:
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            await client.request(
                "DELETE", f"{orchestrator_url}/register/node", json=body
            )
    except Exception as exc:
        logger.warning(
            "unregister failed for %s: %s", body.get("node_name"), exc
        )


def build_node_app(
    *,
    graph_id: str,
    node_name: str,
    handler: NodeHandler,
    orchestrator_url: str,
    public_endpoint: str,
) -> FastAPI:
    register_body = {
        "graph_id": graph_id,
        "node_name": node_name,
        "endpoint": public_endpoint,
    }

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        _init_otel(service_name=f"a2a-{node_name}")
        await _register_with_backoff(orchestrator_url, register_body)
        logger.info(
            "registered graph_id=%s node=%s endpoint=%s",
            graph_id,
            node_name,
            public_endpoint,
        )
        try:
            yield
        finally:
            await _unregister_best_effort(orchestrator_url, register_body)
            logger.info("unregistered node=%s", node_name)

    app = FastAPI(lifespan=lifespan)

    if os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app)

    # WHY no response_model=InvokeResponse: the AMAZE_DEBUG_BAD_PATCH branch below
    # deliberately returns a non-dict state_patch so ST-RLG-6 can verify driver-side
    # InvalidStatePatch handling. Declaring response_model would let FastAPI reject
    # that bad shape server-side and the test would lose its enforcement target.
    @app.post("/invoke")
    async def invoke(req: InvokeRequest) -> dict[str, Any]:
        logger.info(
            "invoke graph_id=%s node_name=%s run_id=%s trace_id=%s",
            req.graph_id,
            req.node_name,
            req.run_id,
            req.trace_id,
        )
        if os.environ.get("AMAZE_DEBUG_BAD_PATCH") == "1":
            return {"state_patch": "not-a-dict"}
        result = await handler(req.state, req.config)
        if not isinstance(result, dict):
            raise HTTPException(status_code=500, detail="handler-returned-non-dict")
        return {"state_patch": result}

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {"status": "ok", "graph_id": graph_id, "node_name": node_name}

    return app


def serve(app: FastAPI, host: str, port: int) -> None:
    uvicorn.run(app, host=host, port=port, log_config=None)
