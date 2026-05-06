from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

import httpx
from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph
from langgraph.types import Command as LGCommand
from langgraph.types import Send as LGSend
from opentelemetry import trace

logger = logging.getLogger(__name__)

_OTEL_INITIALIZED = False

_shared_http_clients: dict[tuple[str, float], httpx.AsyncClient] = {}
_shared_sync_http_clients: dict[tuple[str, float], httpx.Client] = {}


def _get_shared_http_client(url: str, timeout: float) -> httpx.AsyncClient:
    key = (url, timeout)
    if key not in _shared_http_clients:
        _shared_http_clients[key] = httpx.AsyncClient(timeout=timeout)
    return _shared_http_clients[key]


def _get_shared_sync_http_client(url: str, timeout: float) -> httpx.Client:
    key = (url, timeout)
    if key not in _shared_sync_http_clients:
        _shared_sync_http_clients[key] = httpx.Client(timeout=timeout)
    return _shared_sync_http_clients[key]


class AmazeGraphError(Exception):
    pass


class RemoteNodeNotRegistered(AmazeGraphError):
    def __init__(self, graph_id: str, node_name: str) -> None:
        super().__init__(
            f"remote node not registered: graph_id={graph_id} node_name={node_name}"
        )
        self.graph_id = graph_id
        self.node_name = node_name


class RemoteNodeInvokeError(AmazeGraphError):
    def __init__(
        self,
        graph_id: str,
        node_name: str,
        status: int | None,
        body: str,
    ) -> None:
        super().__init__(
            f"remote node invoke failed: graph_id={graph_id} node_name={node_name} "
            f"status={status} body={body[:512]}"
        )
        self.graph_id = graph_id
        self.node_name = node_name
        self.status = status
        self.body = body


class InvalidStatePatch(AmazeGraphError):
    def __init__(self, graph_id: str, node_name: str, payload: Any) -> None:
        super().__init__(
            f"remote node returned invalid state_patch: graph_id={graph_id} "
            f"node_name={node_name} payload={payload!r}"
        )
        self.graph_id = graph_id
        self.node_name = node_name
        self.payload = payload


class OrchestratorUnavailable(AmazeGraphError):
    pass


@dataclass
class AmazeCommand:
    """Return this from a remote node handler to control graph routing.

    Instead of a plain state-patch dict, the handler returns AmazeCommand
    and _common.py translates it to the {"command": ...} wire format. Using
    a typed return avoids any key-collision with state fields named "command".
    """
    goto: "str | list[str]"
    update: "dict | None" = None


@dataclass
class ResolvedNode:
    """Endpoint and optional cache TTL returned by OrchestratorClient.resolve_node."""
    endpoint: str
    cache_ttl: int | None = None


class InvalidCommand(AmazeGraphError):
    def __init__(self, graph_id: str, node_name: str, reason: str) -> None:
        super().__init__(
            f"invalid command from remote node: graph_id={graph_id} "
            f"node_name={node_name} reason={reason}"
        )
        self.graph_id = graph_id
        self.node_name = node_name
        self.reason = reason


class OrchestratorClient:
    def __init__(self, base_url: str, timeout: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url, timeout=self.timeout
            )
        return self._client

    async def __aenter__(self) -> "OrchestratorClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def register_graph(
        self,
        graph_id: str,
        nodes: list[str],
        edges: list[tuple[str, str]],
    ) -> None:
        client = self._get_client()
        body = {
            "graph_id": graph_id,
            "nodes": nodes,
            "edges": [list(e) for e in edges],
        }
        try:
            r = await client.post("/register/graph", json=body)
        except httpx.ConnectError as exc:
            raise OrchestratorUnavailable(
                f"cannot reach orchestrator at {self.base_url}: {exc}"
            ) from exc
        if r.status_code // 100 != 2:
            raise OrchestratorUnavailable(
                f"register_graph failed: status={r.status_code} body={r.text[:512]}"
            )

    async def resolve_node(self, graph_id: str, node_name: str) -> ResolvedNode:
        client = self._get_client()
        try:
            r = await client.get(f"/resolve/node/{graph_id}/{node_name}")
        except httpx.TransportError as exc:
            raise OrchestratorUnavailable(
                f"cannot reach orchestrator at {self.base_url}: {exc}"
            ) from exc
        if r.status_code == 404:
            raise RemoteNodeNotRegistered(graph_id, node_name)
        if r.status_code // 100 != 2:
            raise OrchestratorUnavailable(
                f"resolve_node failed: status={r.status_code} body={r.text[:512]}"
            )
        data = r.json()
        return ResolvedNode(endpoint=data["endpoint"], cache_ttl=data.get("cache_ttl"))

    async def emit_event(self, run_id: str, event: dict) -> None:
        client = self._get_client()
        try:
            r = await client.post(f"/runs/{run_id}/events", json=event)
            if r.status_code // 100 != 2:
                logger.warning(
                    "emit_event non-2xx: run_id=%s status=%s body=%s",
                    run_id,
                    r.status_code,
                    r.text[:256],
                )
        except Exception as exc:
            logger.warning("emit_event failed: run_id=%s err=%s", run_id, exc)

    async def get_cache(self, key: str) -> dict | None:
        """Return cached body dict, or None on miss."""
        client = self._get_client()
        try:
            r = await client.get(f"/cache/{key}")
            if r.status_code == 200:
                data = r.json()
                if data.get("hit"):
                    return data["body"]
        except Exception as exc:
            logger.warning("get_cache failed: key=%s err=%s", key, exc)
        return None

    async def put_cache(self, key: str, body: dict, ttl: int) -> None:
        """Store body in cache with TTL seconds."""
        client = self._get_client()
        try:
            await client.put(f"/cache/{key}", json={"body": body, "ttl": ttl})
        except Exception as exc:
            logger.warning("put_cache failed: key=%s err=%s", key, exc)

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None


def _extract_langsmith_context(cfg: dict) -> dict | None:
    """Extract LangSmith parent_run_id from LangGraph-injected callbacks.

    Returns None when LangSmith is not enabled or callbacks carry no parent run.
    Fully backward-compatible: the returned dict is added as an optional field
    in the /invoke wire payload; remote nodes that don't understand it ignore it.
    """
    callbacks = cfg.get("callbacks")
    if callbacks is None:
        return None
    parent_run_id = getattr(callbacks, "parent_run_id", None)
    if parent_run_id is None:
        return None
    handlers = getattr(callbacks, "handlers", []) or []
    project_name = os.environ.get("LANGCHAIN_PROJECT", "default")
    for h in handlers:
        if hasattr(h, "project_name"):
            project_name = h.project_name
            break
    return {"parent_run_id": str(parent_run_id), "project_name": project_name}


# run_id / trace_id are per-run tracking metadata injected by the driver,
# not business state.  Excluding them ensures repeated calls with identical
# business inputs share a cache entry even across different run IDs.
_CACHE_KEY_EXCLUDE = frozenset({"run_id", "trace_id"})


def _compute_cache_key(graph_id: str, node_name: str, state: dict) -> str:
    state_for_key = {k: v for k, v in state.items() if k not in _CACHE_KEY_EXCLUDE}
    raw = f"{graph_id}:{node_name}:{json.dumps(state_for_key, sort_keys=True, ensure_ascii=True)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:32]


class AmazeGraph:
    def __init__(
        self,
        state_schema: Any,
        *,
        graph_id: str,
        orchestrator_url: str | None = None,
        checkpointer: Any = None,
        sync: bool = False,
        **langgraph_kwargs: Any,
    ) -> None:
        self.graph_id = graph_id
        self._checkpointer = checkpointer
        self._sync = sync
        self.graph = StateGraph(state_schema, **langgraph_kwargs)
        url = orchestrator_url or os.environ.get("AMAZE_ORCHESTRATOR_URL")
        if not url:
            raise ValueError(
                "orchestrator_url not provided and AMAZE_ORCHESTRATOR_URL "
                "env var is not set"
            )
        self.orchestrator_url = url.rstrip("/")
        self.orchestrator = OrchestratorClient(self.orchestrator_url)
        self.remote_nodes: set[str] = set()
        self._nodes: list[str] = []
        self._edges: list[tuple[str, str]] = []
        self._entry: str | None = None

    def add_node(self, name: str, action: Any, *args: Any, **kwargs: Any) -> "AmazeGraph":
        self._nodes.append(name)
        self.graph.add_node(name, action, *args, **kwargs)
        return self

    def remote_node(self, name: str) -> "AmazeGraph":
        self.remote_nodes.add(name)
        self._nodes.append(name)
        proxy = self._make_sync_remote_proxy(name) if self._sync else self._make_remote_proxy(name)
        logger.info("remote_node %r registered as %s proxy (sync=%s)", name, "sync" if self._sync else "async", self._sync)
        self.graph.add_node(name, proxy)
        return self

    def add_edge(self, start: str, end: str) -> "AmazeGraph":
        self._edges.append((start, end))
        self.graph.add_edge(start, end)
        return self

    def add_conditional_edges(
        self,
        source: str,
        path: Any,
        path_map: Any = None,
        then: Any = None,
    ) -> "AmazeGraph":
        # LangGraph 1.x dropped the `then` positional parameter; call with only
        # the args that exist to avoid "takes from 3 to 4 positional arguments".
        if then is not None:
            self.graph.add_conditional_edges(source, path, path_map, then)
        elif path_map is not None:
            self.graph.add_conditional_edges(source, path, path_map)
        else:
            self.graph.add_conditional_edges(source, path)
        return self

    def set_entry_point(self, node: str) -> "AmazeGraph":
        self._entry = node
        self.graph.set_entry_point(node)
        return self

    def compile(self, *args: Any, **kwargs: Any) -> Any:
        body = {
            "graph_id": self.graph_id,
            "nodes": list(self._nodes),
            "edges": [list(e) for e in self._edges],
        }
        try:
            r = httpx.post(
                f"{self.orchestrator_url}/register/graph",
                json=body,
                timeout=10.0,
            )
        except httpx.ConnectError as exc:
            raise OrchestratorUnavailable(
                f"cannot reach orchestrator at {self.orchestrator_url}: {exc}"
            ) from exc
        if r.status_code // 100 != 2:
            raise OrchestratorUnavailable(
                f"register_graph failed: status={r.status_code} body={r.text[:512]}"
            )
        if self._checkpointer is not None:
            kwargs.setdefault("checkpointer", self._checkpointer)
        return self.graph.compile(*args, **kwargs)

    def _get_http_client(self) -> httpx.AsyncClient:
        timeout = float(os.environ.get("AMAZE_NODE_INVOKE_TIMEOUT", "30.0"))
        return _get_shared_http_client(self.orchestrator_url, timeout)

    def _get_sync_http_client(self) -> httpx.Client:
        timeout = float(os.environ.get("AMAZE_NODE_INVOKE_TIMEOUT", "30.0"))
        return _get_shared_sync_http_client(self.orchestrator_url, timeout)

    def _make_event(
        self,
        event_type: str,
        node_name: str,
        trace_id: str | None,
        status: str | None = None,
        error: str | None = None,
        error_kind: str | None = None,
    ) -> dict:
        event: dict = {
            "event": event_type,
            "graph_id": self.graph_id,
            "node_name": node_name,
            "trace_id": trace_id,
            "status": status,
            "error": error,
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        if error_kind is not None:
            event["error_kind"] = error_kind
        return event

    async def _parse_remote_body(
        self,
        body: dict,
        node_name: str,
        run_id: str | None,
        trace_id: str | None,
    ) -> "dict | LGCommand":
        """Parse a wire response body (from live invocation or cache) into the
        value LangGraph expects: a state-patch dict or LGCommand.

        Identical validation runs for both cached and live responses so a
        cached Command is reconstructed correctly rather than returned raw.
        """
        # ── Command detection (Cases 14 + 15) ──────────────────────
        command_raw = body.get("command")
        if command_raw is not None:
            if not isinstance(command_raw, dict):
                if run_id:
                    await self.orchestrator.emit_event(
                        run_id,
                        self._make_event(
                            "node-error", node_name, trace_id,
                            status="error",
                            error="invalid-command-shape",
                            error_kind="proxy_block",
                        ),
                    )
                raise InvalidCommand(self.graph_id, node_name, "command must be a dict")

            goto_raw = command_raw.get("goto")
            if not goto_raw and goto_raw != 0:
                if run_id:
                    await self.orchestrator.emit_event(
                        run_id,
                        self._make_event(
                            "node-error", node_name, trace_id,
                            status="error",
                            error="invalid-command-empty-goto",
                            error_kind="proxy_block",
                        ),
                    )
                raise InvalidCommand(
                    self.graph_id, node_name, "command.goto is required and non-empty"
                )

            if isinstance(goto_raw, str):
                reconstructed = [goto_raw]
                scalar = True
            elif isinstance(goto_raw, dict) and goto_raw.get("__send__"):
                reconstructed = [LGSend(goto_raw["node"], goto_raw["arg"])]
                scalar = False
            elif isinstance(goto_raw, list):
                reconstructed = [
                    LGSend(item["node"], item["arg"])
                    if isinstance(item, dict) and item.get("__send__")
                    else item
                    for item in goto_raw
                ]
                scalar = False
            else:
                if run_id:
                    await self.orchestrator.emit_event(
                        run_id,
                        self._make_event(
                            "node-error", node_name, trace_id,
                            status="error",
                            error=f"invalid-command-goto-type:{type(goto_raw).__name__}",
                            error_kind="proxy_block",
                        ),
                    )
                raise InvalidCommand(
                    self.graph_id, node_name,
                    f"command.goto must be str or list, got {type(goto_raw).__name__}",
                )

            known = set(self._nodes) | {"__end__"}
            for item in reconstructed:
                target_name = item.node if isinstance(item, LGSend) else item
                if target_name not in known:
                    if run_id:
                        await self.orchestrator.emit_event(
                            run_id,
                            self._make_event(
                                "node-error", node_name, trace_id,
                                status="error",
                                error=f"unknown-goto-target:{target_name}",
                                error_kind="proxy_block",
                            ),
                        )
                    raise InvalidCommand(
                        self.graph_id, node_name,
                        f"command.goto target {target_name!r} not in graph",
                    )

            update_patch = command_raw.get("update") or {}
            if not isinstance(update_patch, dict):
                if run_id:
                    await self.orchestrator.emit_event(
                        run_id,
                        self._make_event(
                            "node-error", node_name, trace_id,
                            status="error",
                            error="invalid-command-update-type",
                            error_kind="proxy_block",
                        ),
                    )
                raise InvalidCommand(
                    self.graph_id, node_name, "command.update must be a dict"
                )

            if run_id:
                await self.orchestrator.emit_event(
                    run_id,
                    self._make_event("node-exit", node_name, trace_id, status="ok"),
                )
            return LGCommand(update=update_patch, goto=goto_raw if scalar else reconstructed)

        # ── State-patch path ─────────────────────────────────────────
        state_patch = body.get("state_patch")
        if not isinstance(state_patch, dict):
            if run_id:
                await self.orchestrator.emit_event(
                    run_id,
                    self._make_event(
                        "node-error", node_name, trace_id,
                        status="error",
                        error="invalid-state-patch",
                        error_kind="node_error",
                    ),
                )
            raise InvalidStatePatch(self.graph_id, node_name, body)

        if run_id:
            await self.orchestrator.emit_event(
                run_id,
                self._make_event("node-exit", node_name, trace_id, status="ok"),
            )
        return state_patch

    def _sync_emit_event(self, client: httpx.Client, run_id: str, event: dict) -> None:
        try:
            client.post(f"{self.orchestrator_url}/runs/{run_id}/events", json=event)
        except Exception:
            pass  # events are best-effort; never break the node call

    def _parse_remote_body_sync(
        self,
        body: dict,
        node_name: str,
        run_id: str | None,
        trace_id: str | None,
        client: httpx.Client,
    ) -> "dict | LGCommand":
        command_raw = body.get("command")
        if command_raw is not None:
            if not isinstance(command_raw, dict):
                if run_id:
                    self._sync_emit_event(client, run_id, self._make_event(
                        "node-error", node_name, trace_id,
                        status="error", error="invalid-command-shape", error_kind="proxy_block",
                    ))
                raise InvalidCommand(self.graph_id, node_name, "command must be a dict")

            goto_raw = command_raw.get("goto")
            if not goto_raw and goto_raw != 0:
                if run_id:
                    self._sync_emit_event(client, run_id, self._make_event(
                        "node-error", node_name, trace_id,
                        status="error", error="invalid-command-empty-goto", error_kind="proxy_block",
                    ))
                raise InvalidCommand(self.graph_id, node_name, "command.goto is required and non-empty")

            if isinstance(goto_raw, str):
                reconstructed = [goto_raw]
                scalar = True
            elif isinstance(goto_raw, dict) and goto_raw.get("__send__"):
                reconstructed = [LGSend(goto_raw["node"], goto_raw["arg"])]
                scalar = False
            elif isinstance(goto_raw, list):
                reconstructed = [
                    LGSend(item["node"], item["arg"])
                    if isinstance(item, dict) and item.get("__send__")
                    else item
                    for item in goto_raw
                ]
                scalar = False
            else:
                if run_id:
                    self._sync_emit_event(client, run_id, self._make_event(
                        "node-error", node_name, trace_id,
                        status="error",
                        error=f"invalid-command-goto-type:{type(goto_raw).__name__}",
                        error_kind="proxy_block",
                    ))
                raise InvalidCommand(
                    self.graph_id, node_name,
                    f"command.goto must be str or list, got {type(goto_raw).__name__}",
                )

            known = set(self._nodes) | {"__end__"}
            for item in reconstructed:
                target_name = item.node if isinstance(item, LGSend) else item
                if target_name not in known:
                    if run_id:
                        self._sync_emit_event(client, run_id, self._make_event(
                            "node-error", node_name, trace_id,
                            status="error",
                            error=f"unknown-goto-target:{target_name}",
                            error_kind="proxy_block",
                        ))
                    raise InvalidCommand(
                        self.graph_id, node_name,
                        f"command.goto target {target_name!r} not in graph",
                    )

            update_patch = command_raw.get("update") or {}
            if not isinstance(update_patch, dict):
                if run_id:
                    self._sync_emit_event(client, run_id, self._make_event(
                        "node-error", node_name, trace_id,
                        status="error", error="invalid-command-update-type", error_kind="proxy_block",
                    ))
                raise InvalidCommand(self.graph_id, node_name, "command.update must be a dict")

            if run_id:
                self._sync_emit_event(client, run_id, self._make_event("node-exit", node_name, trace_id, status="ok"))
            return LGCommand(update=update_patch, goto=goto_raw if scalar else reconstructed)

        state_patch = body.get("state_patch")
        if not isinstance(state_patch, dict):
            if run_id:
                self._sync_emit_event(client, run_id, self._make_event(
                    "node-error", node_name, trace_id,
                    status="error", error="invalid-state-patch", error_kind="node_error",
                ))
            raise InvalidStatePatch(self.graph_id, node_name, body)

        if run_id:
            self._sync_emit_event(client, run_id, self._make_event("node-exit", node_name, trace_id, status="ok"))
        return state_patch

    def _make_sync_remote_proxy(self, node_name: str) -> Callable[..., Any]:
        def remote_proxy(state: dict, config=None) -> dict:
            try:
                from langgraph.config import get_config as _lg_get_config
                _ctx_cfg = _lg_get_config()
                cfg: dict = dict(_ctx_cfg) if _ctx_cfg else (dict(config) if isinstance(config, dict) else {})
            except (ImportError, RuntimeError):
                cfg = dict(config) if isinstance(config, dict) else {}
            metadata = cfg.get("metadata") or {}

            run_id = (state.get("run_id") if isinstance(state, dict) else None) or metadata.get("run_id")
            trace_id = (state.get("trace_id") if isinstance(state, dict) else None) or metadata.get("trace_id")

            raw_configurable = cfg.get("configurable") or {}
            runtime_context = raw_configurable.get("__amaze_runtime_context__") or {}
            if not isinstance(runtime_context, dict):
                runtime_context = {}

            _skip_prefixes = ("__pregel_", "checkpoint_")
            clean_configurable: dict[str, Any] = {}
            for _k, _v in raw_configurable.items():
                if any(_k.startswith(_p) for _p in _skip_prefixes):
                    continue
                try:
                    json.dumps(_v)
                    clean_configurable[_k] = _v
                except (TypeError, ValueError):
                    pass

            config_subset_raw = {
                "tags": cfg.get("tags"),
                "metadata": cfg.get("metadata"),
                "configurable": clean_configurable if clean_configurable else None,
                "run_name": cfg.get("run_name"),
                "recursion_limit": cfg.get("recursion_limit"),
            }
            config_subset = {k: v for k, v in config_subset_raw.items() if v is not None}

            langsmith_ctx = _extract_langsmith_context(cfg)

            logger.info("▶ [%s] invoking remote node sync (graph=%s)", node_name, self.graph_id)
            client = self._get_sync_http_client()

            try:
                r = client.get(f"{self.orchestrator_url}/resolve/node/{self.graph_id}/{node_name}")
            except httpx.ConnectError as exc:
                raise RemoteNodeInvokeError(self.graph_id, node_name, None, str(exc)) from exc

            if r.status_code == 404:
                raise RemoteNodeNotRegistered(self.graph_id, node_name)
            if r.status_code // 100 != 2:
                raise RemoteNodeInvokeError(self.graph_id, node_name, r.status_code, r.text)

            resolved_data = r.json()
            endpoint = resolved_data["endpoint"]

            if run_id:
                self._sync_emit_event(client, run_id, self._make_event("node-enter", node_name, trace_id))

            payload = {
                "graph_id": self.graph_id,
                "node_name": node_name,
                "run_id": run_id,
                "trace_id": trace_id,
                "state": state,
                "config": config_subset,
                "runtime_context": runtime_context,
                "langsmith_context": langsmith_ctx,
            }

            try:
                response = client.post(endpoint, json=payload)
            except httpx.TimeoutException as exc:
                if run_id:
                    self._sync_emit_event(client, run_id, self._make_event(
                        "node-error", node_name, trace_id,
                        status="error", error=f"timeout: {exc}", error_kind="timeout",
                    ))
                raise RemoteNodeInvokeError(self.graph_id, node_name, None, str(exc)) from exc
            except httpx.TransportError as exc:
                if run_id:
                    self._sync_emit_event(client, run_id, self._make_event(
                        "node-error", node_name, trace_id,
                        status="error", error=f"transport: {exc}", error_kind="proxy_block",
                    ))
                raise RemoteNodeInvokeError(self.graph_id, node_name, None, str(exc)) from exc

            if response.status_code // 100 != 2:
                if run_id:
                    self._sync_emit_event(client, run_id, self._make_event(
                        "node-error", node_name, trace_id,
                        status="error", error=f"http-{response.status_code}", error_kind="node_error",
                    ))
                raise RemoteNodeInvokeError(self.graph_id, node_name, response.status_code, response.text)

            try:
                body = response.json()
            except json.JSONDecodeError as exc:
                if run_id:
                    self._sync_emit_event(client, run_id, self._make_event(
                        "node-error", node_name, trace_id,
                        status="error", error="invalid-json", error_kind="node_error",
                    ))
                raise InvalidStatePatch(self.graph_id, node_name, response.text) from exc

            return self._parse_remote_body_sync(body, node_name, run_id, trace_id, client)

        return remote_proxy

    def _make_remote_proxy(
        self, node_name: str
    ) -> Callable[..., Awaitable[Any]]:
        async def remote_proxy(
            state: dict,
            config: RunnableConfig | None = None,
        ) -> dict:
            # LangGraph 1.x injects config via a ContextVar regardless of whether
            # parameter injection works for closures.  Prefer the ContextVar value
            # so we always get the full config (thread_id, tags, metadata, etc.).
            try:
                from langgraph.config import get_config as _lg_get_config
                _ctx_cfg = _lg_get_config()
                cfg: dict = dict(_ctx_cfg) if _ctx_cfg else (dict(config) if isinstance(config, dict) else {})
            except (ImportError, RuntimeError):
                cfg: dict = dict(config) if isinstance(config, dict) else {}
            metadata = cfg.get("metadata") or {}

            run_id = (state.get("run_id") if isinstance(state, dict) else None) or metadata.get("run_id")
            trace_id = (state.get("trace_id") if isinstance(state, dict) else None) or metadata.get("trace_id")

            # §9.3 runtime_context: extracted BEFORE cleaning configurable so
            # the __amaze_runtime_context__ value is always accessible even if
            # the broader configurable dict contains non-serializable entries.
            raw_configurable = cfg.get("configurable") or {}
            runtime_context = raw_configurable.get("__amaze_runtime_context__") or {}
            if not isinstance(runtime_context, dict):
                runtime_context = {}

            # Strip LangGraph-internal configurable keys (__pregel_*, checkpoint_*)
            # that are not JSON-serializable and must not cross process boundaries.
            _skip_prefixes = ("__pregel_", "checkpoint_")
            clean_configurable: dict[str, Any] = {}
            for _k, _v in raw_configurable.items():
                if any(_k.startswith(_p) for _p in _skip_prefixes):
                    continue
                try:
                    json.dumps(_v)
                    clean_configurable[_k] = _v
                except (TypeError, ValueError):
                    pass  # drop non-serializable values silently

            config_subset_raw = {
                "tags": cfg.get("tags"),
                "metadata": cfg.get("metadata"),
                "configurable": clean_configurable if clean_configurable else None,
                "run_name": cfg.get("run_name"),
                "recursion_limit": cfg.get("recursion_limit"),
            }
            config_subset = {k: v for k, v in config_subset_raw.items() if v is not None}

            langsmith_ctx = _extract_langsmith_context(cfg)

            logger.info("▶ [%s] invoking remote node (graph=%s)", node_name, self.graph_id)
            tracer = trace.get_tracer(__name__)
            with tracer.start_as_current_span("amazegraph.invoke_remote") as span:
                span.set_attribute("amaze.graph_id", self.graph_id)
                span.set_attribute("amaze.node_name", node_name)
                if run_id:
                    span.set_attribute("amaze.run_id", run_id)
                if trace_id:
                    span.set_attribute("amaze.trace_id", trace_id)

                try:
                    resolved = await self.orchestrator.resolve_node(
                        self.graph_id, node_name
                    )
                except RemoteNodeNotRegistered as exc:
                    if run_id:
                        await self.orchestrator.emit_event(
                            run_id,
                            self._make_event(
                                "node-error",
                                node_name,
                                trace_id,
                                status="error",
                                error="node-not-registered",
                                error_kind="proxy_block",
                            ),
                        )
                    raise

                cache_key: str | None = None
                if resolved.cache_ttl is not None:
                    cache_key = _compute_cache_key(self.graph_id, node_name, state)
                    cached_body = await self.orchestrator.get_cache(cache_key)
                    if cached_body is not None:
                        span.set_attribute("amaze.cache_hit", True)
                        logger.info("◀ [%s] cache hit (graph=%s)", node_name, self.graph_id)
                        # run_id=None suppresses all Redis stream events on cache hit —
                        # cache hits are observable via amaze.cache_hit OTEL attribute only
                        return await self._parse_remote_body(
                            cached_body, node_name, None, None
                        )

                if run_id:
                    await self.orchestrator.emit_event(
                        run_id,
                        self._make_event("node-enter", node_name, trace_id),
                    )

                payload = {
                    "graph_id": self.graph_id,
                    "node_name": node_name,
                    "run_id": run_id,
                    "trace_id": trace_id,
                    "state": state,
                    "config": config_subset,
                    "runtime_context": runtime_context,
                    "langsmith_context": langsmith_ctx,
                }

                client = self._get_http_client()
                try:
                    response = await client.post(resolved.endpoint, json=payload)
                except httpx.TimeoutException as exc:
                    if run_id:
                        await self.orchestrator.emit_event(
                            run_id,
                            self._make_event(
                                "node-error",
                                node_name,
                                trace_id,
                                status="error",
                                error=f"timeout: {exc}",
                                error_kind="timeout",
                            ),
                        )
                    raise RemoteNodeInvokeError(
                        self.graph_id, node_name, None, str(exc)
                    ) from exc
                except httpx.TransportError as exc:
                    if run_id:
                        await self.orchestrator.emit_event(
                            run_id,
                            self._make_event(
                                "node-error",
                                node_name,
                                trace_id,
                                status="error",
                                error=f"transport: {exc}",
                                error_kind="proxy_block",
                            ),
                        )
                    raise RemoteNodeInvokeError(
                        self.graph_id, node_name, None, str(exc)
                    ) from exc

                if response.status_code // 100 != 2:
                    body_text = response.text
                    if run_id:
                        await self.orchestrator.emit_event(
                            run_id,
                            self._make_event(
                                "node-error",
                                node_name,
                                trace_id,
                                status="error",
                                error=f"http-{response.status_code}",
                                error_kind="node_error",
                            ),
                        )
                    raise RemoteNodeInvokeError(
                        self.graph_id, node_name, response.status_code, body_text
                    )

                try:
                    body = response.json()
                except json.JSONDecodeError as exc:
                    if run_id:
                        await self.orchestrator.emit_event(
                            run_id,
                            self._make_event(
                                "node-error",
                                node_name,
                                trace_id,
                                status="error",
                                error="invalid-json",
                                error_kind="node_error",
                            ),
                        )
                    raise InvalidStatePatch(
                        self.graph_id, node_name, response.text
                    ) from exc

                result = await self._parse_remote_body(body, node_name, run_id, trace_id)
                if cache_key is not None and resolved.cache_ttl is not None:
                    await self.orchestrator.put_cache(cache_key, body, resolved.cache_ttl)
                return result

        return remote_proxy

    async def aclose(self) -> None:
        await self.orchestrator.close()


def _init_otel(service_name: str) -> None:
    global _OTEL_INITIALIZED
    if _OTEL_INITIALIZED:
        return
    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    if not endpoint:
        return

    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    HTTPXClientInstrumentor().instrument()
    _OTEL_INITIALIZED = True
