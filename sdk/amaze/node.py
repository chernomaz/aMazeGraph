from __future__ import annotations

import asyncio
import inspect
import logging
import os
import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any, AsyncIterator, Awaitable, Callable

import httpx
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from langgraph.types import Command, Send
from sdk.amaze.langgraph import _init_otel

logger = logging.getLogger(__name__)

NodeHandler = Callable[..., Awaitable[dict]]


class RuntimeNotAvailable(Exception):
    """Raised when a remote node tries to use a Runtime feature that is not
    available in distributed mode (e.g. runtime.store, runtime.stream_writer).
    """


class Runtime:
    """Stub Runtime exposed to remote node handlers.

    Only `runtime.context` is populated from the wire `runtime_context` field.
    `runtime.store` (capability #22) and `runtime.stream_writer` (capability
    #24) are not available across the wire — accessing them raises
    `RuntimeNotAvailable`.
    """

    def __init__(self, context: dict | None = None) -> None:
        self._context = SimpleNamespace(**(context or {}))

    @property
    def context(self) -> SimpleNamespace:
        return self._context

    @property
    def store(self) -> Any:
        raise RuntimeNotAvailable(
            "runtime.store not available in distributed mode (capability #22)"
        )

    @property
    def stream_writer(self) -> Any:
        raise RuntimeNotAvailable(
            "runtime.stream_writer not available in distributed mode (capability #24)"
        )


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


def _serialize_goto(goto: Any) -> Any:
    if isinstance(goto, Send):
        return {"__send__": True, "node": goto.node, "arg": goto.arg}
    if isinstance(goto, list):
        return [_serialize_goto(item) for item in goto]
    return goto


class InvokeRequest(BaseModel):
    graph_id: str
    node_name: str
    run_id: str | None = None
    trace_id: str | None = None
    state: dict[str, Any]
    config: dict[str, Any] = {}
    runtime_context: dict[str, Any] = {}
    langsmith_context: dict[str, Any] | None = None


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
            except httpx.TransportError as exc:
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


_REGISTERED_HANDLERS: list[tuple[str, str, NodeHandler, int | None]] = []


def _handler_accepts_runtime(handler: NodeHandler) -> bool:
    """Return True if the handler signature has a 3rd parameter named
    `runtime` or annotated `Runtime`. Used to decide whether to inject the
    Runtime stub when dispatching /invoke.
    """
    try:
        sig = inspect.signature(handler)
    except (TypeError, ValueError):
        return False
    params = [
        p
        for p in sig.parameters.values()
        if p.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.KEYWORD_ONLY,
        )
    ]
    for p in params:
        if p.name == "runtime":
            return True
        ann = p.annotation
        if ann is Runtime:
            return True
        if isinstance(ann, str) and ann == "Runtime":
            return True
    return False


def remote_node(*, graph_id: str, node_name: str, cache_ttl: int | None = None):
    """Decorator that registers an async handler as a remote node.

    Multiple handlers may be decorated in a single Python process; serve_node()
    will host all of them under one FastAPI app and dispatch /invoke calls by
    (graph_id, node_name).
    """

    def wrapper(handler: NodeHandler) -> NodeHandler:
        _REGISTERED_HANDLERS.append((graph_id, node_name, handler, cache_ttl))
        return handler

    return wrapper


def _build_handlers_app(
    *,
    handlers: list[tuple[str, str, NodeHandler, int | None]],
    orchestrator_url: str,
    public_endpoint: str,
    service_name: str,
) -> FastAPI:
    handlers_map: dict[tuple[str, str], NodeHandler] = {
        (gid, nname): h for gid, nname, h, _ttl in handlers
    }
    register_bodies = []
    for gid, nname, _, ttl in handlers:
        body: dict[str, Any] = {"graph_id": gid, "node_name": nname, "endpoint": public_endpoint}
        if ttl is not None:
            body["cache_ttl"] = ttl
        register_bodies.append(body)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        _init_otel(service_name=service_name)
        for body in register_bodies:
            await _register_with_backoff(orchestrator_url, body)
            logger.info(
                "registered graph_id=%s node=%s endpoint=%s",
                body["graph_id"],
                body["node_name"],
                public_endpoint,
            )
        try:
            yield
        finally:
            for body in register_bodies:
                await _unregister_best_effort(orchestrator_url, body)
                logger.info("unregistered node=%s", body["node_name"])

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
            "invoke graph_id=%s node_name=%s run_id=%s trace_id=%s "
            "config_keys=%s runtime_context=%s",
            req.graph_id,
            req.node_name,
            req.run_id,
            req.trace_id,
            list(req.config.keys()),
            req.runtime_context,
        )
        if os.environ.get("AMAZE_DEBUG_BAD_PATCH") == "1":
            return {"state_patch": "not-a-dict"}
        handler = handlers_map.get((req.graph_id, req.node_name))
        if handler is None:
            raise HTTPException(
                status_code=404,
                detail=f"no handler registered for graph_id={req.graph_id} node_name={req.node_name}",
            )
        handler_config = dict(req.config)
        if req.langsmith_context and req.langsmith_context.get("parent_run_id"):
            try:
                import uuid as _uuid
                from langchain_core.callbacks import CallbackManager
                from langchain_core.tracers.langchain import LangChainTracer
                _parent_run_id = _uuid.UUID(req.langsmith_context["parent_run_id"])
                # LangChainTracer does not accept parent_run_id in its constructor;
                # parent_run_id must be set on the CallbackManager so it is passed
                # as parent_run_id in on_llm_start / on_chain_start callbacks.
                _tracer = LangChainTracer(
                    project_name=req.langsmith_context.get("project_name"),
                )
                _cb_manager = CallbackManager(
                    handlers=[_tracer],
                    parent_run_id=_parent_run_id,
                )
                handler_config["callbacks"] = _cb_manager
                logger.info(
                    "LangSmith parent_run_id=%s project=%s",
                    req.langsmith_context["parent_run_id"],
                    req.langsmith_context.get("project_name"),
                )
            except Exception as _exc:
                logger.debug("LangSmith tracer setup skipped: %s", _exc)
        if _handler_accepts_runtime(handler):
            runtime = Runtime(req.runtime_context or {})
            result = await handler(req.state, handler_config, runtime)
        else:
            result = await handler(req.state, handler_config)
        # langgraph.types.Command return → translate to wire {"command": {...}} (Cases 14+15).
        # isinstance check is collision-safe: plain dicts (even with a "command" key)
        # always go through the state_patch path below.
        if isinstance(result, Command):
            return {
                "command": {
                    "update": result.update if isinstance(result.update, dict) else {},
                    "goto": _serialize_goto(result.goto),
                }
            }
        # bare Send / list[Send] — normalize to Command(goto=[...]) with no update
        if isinstance(result, (Send, list)):
            goto = [result] if isinstance(result, Send) else result
            return {"command": {"update": {}, "goto": _serialize_goto(goto)}}
        if not isinstance(result, dict):
            raise HTTPException(status_code=500, detail="handler-returned-non-dict")
        return {"state_patch": result}

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {
            "status": "ok",
            "handlers": [
                {"graph_id": gid, "node_name": nname} for gid, nname, _, _ttl in handlers
            ],
        }

    return app


def serve_node(
    *,
    host: str | None = None,
    port: int | None = None,
    public_host: str | None = None,
    public_endpoint: str | None = None,
    orchestrator_url: str | None = None,
) -> None:
    """Start a FastAPI server hosting every @remote_node-decorated handler in this process.

    Resolution order for each parameter: explicit argument → env var → default.
    Env vars: A2A_NODE_HOST, A2A_NODE_PORT, A2A_NODE_PUBLIC_HOST,
    A2A_NODE_PUBLIC_ENDPOINT, AMAZE_ORCHESTRATOR_URL, OTEL_SERVICE_NAME.
    """
    if not _REGISTERED_HANDLERS:
        raise RuntimeError(
            "no @remote_node handlers registered; "
            "decorate at least one async function before calling serve_node()"
        )

    host = host or os.environ.get("A2A_NODE_HOST", "0.0.0.0")
    if port is None:
        port_env = os.environ.get("A2A_NODE_PORT")
        if not port_env:
            raise RuntimeError("port must be set via argument or A2A_NODE_PORT env var")
        port = int(port_env)

    if public_endpoint is None:
        public_endpoint = os.environ.get("A2A_NODE_PUBLIC_ENDPOINT")
    if public_endpoint is None:
        ph = public_host or os.environ.get("A2A_NODE_PUBLIC_HOST", "localhost")
        public_endpoint = f"http://{ph}:{port}/invoke"

    orchestrator_url = orchestrator_url or os.environ.get(
        "AMAZE_ORCHESTRATOR_URL", "http://localhost:8001"
    )

    service_name = os.environ.get("OTEL_SERVICE_NAME") or (
        "a2a-" + "-".join(sorted({n for _, n, _, _ in _REGISTERED_HANDLERS}))
    )
    setup_logging(service_name)

    app = _build_handlers_app(
        handlers=list(_REGISTERED_HANDLERS),
        orchestrator_url=orchestrator_url,
        public_endpoint=public_endpoint,
        service_name=service_name,
    )
    uvicorn.run(app, host=host, port=port, log_config=None)
