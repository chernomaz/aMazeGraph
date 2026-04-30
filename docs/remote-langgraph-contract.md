# Remote LangGraph Node Execution — Sprint 1 Contract

This document is the binding contract between the four moving parts in Sprint 1:

```
┌──────────────────────┐       ┌────────────────────┐       ┌──────────────────────┐
│  main-langgraph      │       │   orchestrator     │       │   a2a node           │
│  (driver, AmazeGraph)│──────▶│   (FastAPI+Redis)  │◀──────│  (FastAPI on host)   │
│                      │       │                    │       │                      │
│  app.ainvoke(state)  │       │  /register/node    │       │  POST /invoke        │
│   resolves nodes  ───┼──┐    │  /resolve/node/... │       │  returns state_patch │
│   POSTs /invoke   ───┼──┼───▶│  /register/graph   │       └──────────────────────┘
│                      │  │    │  /runs/{id}/events │              ▲
│                      │  └────┴──── HTTP ──────────┴──────────────┘
└──────────────────────┘
```

Anything outside this document is **out of scope for Sprint 1** — see the
*Out of scope* section at the bottom.

---

## 1. Identifiers

| Field | Required | Format | Origin | Used by |
|---|---|---|---|---|
| `graph_id` | yes | string, 1–128 chars, `[a-z0-9_-]+` | author of the LangGraph program | registration, resolution, manifest, run events |
| `node_name` | yes | string, 1–64 chars, matches a `StateGraph.add_node` name | author | registration, resolution, run events |
| `endpoint` | yes | absolute URL ending in the node's invoke path, e.g. `http://a2a-research:9002/invoke` | the remote node at startup | resolution, proxy invoke |
| `run_id` | yes | string supplied by the caller in initial state | end user / driver | run event grouping, log correlation |
| `trace_id` | yes | string supplied by the caller in initial state OR W3C `traceparent` header | end user / driver | OTel span linkage, log correlation |

`graph_id + node_name` is the **unique routing key**. Re-registering the same
key from a different host is **last-write-wins**, with no error and no warning.
This is a known footgun deferred to a future sprint (where a `node_id` or health
check will discriminate hosts).

---

## 2. Endpoints — orchestrator

All requests/responses are `application/json` unless noted. Errors follow
FastAPI's default shape (`{"detail": "..."}`).

### 2.1 `POST /register/node`

Idempotent. Persists `graph_node:{graph_id}:{node_name}` in Redis with **no
TTL**. The remote node is responsible for calling `DELETE /register/node`
during graceful shutdown.

```http
POST /register/node
Content-Type: application/json

{
  "graph_id": "demo_graph_v1",
  "node_name": "research",
  "endpoint": "http://a2a-research:9002/invoke"
}
```
Response 200:
```json
{ "status": "ok", "graph_id": "demo_graph_v1", "node_name": "research" }
```
Response 422: validation error (bad shape).
Response 503: Redis unavailable.

### 2.2 `DELETE /register/node`

Same body. Removes the Redis key. Returns 200 with `{"status":"ok"}` even if
the key was already absent (idempotent).

### 2.3 `GET /resolve/node/{graph_id}/{node_name}`

Response 200:
```json
{ "graph_id": "demo_graph_v1", "node_name": "research",
  "endpoint": "http://a2a-research:9002/invoke" }
```
Response 404: node not registered. Body: `{"detail":"node-not-registered","graph_id":...,"node_name":...}`.

### 2.4 `POST /register/graph`

Called by `AmazeGraph.compile()`. Persists `graph:{graph_id}` (no TTL).

```json
{
  "graph_id": "demo_graph_v1",
  "nodes": ["start", "research", "writer"],
  "edges": [["start","research"], ["research","writer"], ["writer","__end__"]]
}
```
Response 200: `{"status":"ok","graph_id":...}`.
Last-write-wins on `graph_id`.

### 2.5 `GET /graphs/{graph_id}`

Response 200: full manifest object. Response 404 if unregistered.

### 2.6 `POST /runs/{run_id}/events`

Append to the run's Redis Stream `run:{run_id}:events`. Also lazily creates
`run:{run_id}:meta` on first event.

```json
{
  "event": "node-enter",
  "graph_id": "demo_graph_v1",
  "node_name": "research",
  "trace_id": "trace-1",
  "status": null,
  "error": null,
  "ts": "2026-04-29T12:00:00Z"
}
```
Allowed `event` values in Sprint 1:
`run-start`, `node-enter`, `node-exit`, `node-error`, `run-end`.

Response 200: `{"status":"ok","stream_id":"<XADD id>"}`.

### 2.7 `GET /runs/{run_id}`

Response 200:
```json
{
  "meta": {"graph_id":"...", "trace_id":"...", "started_at":"...", "status":"running|done|failed"},
  "events": [ {...XADD entry...}, ... ]
}
```
Response 404 if unknown.

### 2.8 `GET /health`

Response 200: `{"status":"ok","redis":"ok"}`.
Response 503 with `{"status":"degraded","redis":"down"}` if Redis ping fails.

---

## 3. Endpoint — remote A2A node

### 3.1 `POST /invoke`

Request body:
```json
{
  "graph_id": "demo_graph_v1",
  "node_name": "research",
  "run_id": "run-1",
  "trace_id": "trace-1",
  "state": { "...": "full graph state, JSON-serializable" },
  "config": {
    "tags": ["..."],
    "metadata": { "...": "..." },
    "configurable": { "...": "..." },
    "run_name": null,
    "recursion_limit": 25
  }
}
```

Response 200:
```json
{ "state_patch": { "research_result": "..." } }
```

Rules:
- `state_patch` MUST be a JSON object (dict). Lists, scalars, null → invalid.
- The proxy treats non-2xx and non-dict responses as failures and emits
  `node-error` events on the run stream.
- Future sprints may add sibling keys (`command`, `interrupt`, …). Sprint 1
  clients **must ignore unknown keys** rather than reject the response.

### 3.2 `GET /healthz`

Response 200: `{"status":"ok","graph_id":"...","node_name":"..."}`.
Used by orchestrator-side health checks in a future sprint; for Sprint 1, the
docker-compose healthcheck uses this endpoint to gate `main-langgraph`
startup.

### 3.3 Lifespan

On startup:
1. Initialize OTel SDK with service-name `a2a-{node_name}` and OTLP endpoint.
2. POST `/register/node` to the orchestrator with retry-with-backoff (HTTP
   4xx → fail fast; connect errors → up to 5 retries with 2s delay).
3. Start uvicorn server.

On shutdown (SIGTERM, SIGINT, lifespan exit):
1. DELETE `/register/node` to the orchestrator (best-effort, swallow errors).
2. OTel SDK flush + shutdown.

---

## 4. LangGraph object propagation

| Object | On the wire? | Reason |
|---|---|---|
| `state` (TypedDict) | yes — full pass-through | the data the graph operates on |
| `config: RunnableConfig` | partially — `tags`, `metadata`, `configurable`, `run_name`, `recursion_limit` only | other fields hold live Python objects (callbacks, store, checkpointer) that can't be serialized |
| `runtime: Runtime` | no | `runtime.store`, `stream_writer`, `previous` are process-local references |

### Sprint 1 limitations (documented for users)

- Remote nodes cannot use `runtime.store` or `runtime.stream_writer`. Calls to
  these from inside a remote node will raise `AttributeError` against a `None`.
- Local `BaseCallbackHandler` instances registered on the driver do **not**
  fire for events that occur inside remote nodes. OTel spans + the run-event
  Redis Stream are the cross-process observability substitute.
- All values inside `state`, `config["configurable"]`, and `config["metadata"]`
  must be JSON-serializable: no `datetime`, `Decimal`, `Path`, custom classes.

---

## 5. Redis schema

| Key | Type | Value | Lifecycle |
|---|---|---|---|
| `graph_node:{graph_id}:{node_name}` | string | JSON `{"endpoint": "...", "registered_at": "ISO8601"}` | SET on register, DEL on unregister, no TTL |
| `graph:{graph_id}` | string | JSON `{"nodes": [...], "edges": [...], "registered_at": "ISO8601"}` | SET on `compile()`, persists |
| `run:{run_id}:meta` | string | JSON `{"graph_id":"...", "trace_id":"...", "started_at":"ISO8601", "status":"running|done|failed"}` | SET on first event, updated on `run-end` |
| `run:{run_id}:events` | stream | XADD entries with fields `event`, `graph_id`, `node_name`, `trace_id`, `status`, `error`, `ts` | append-only |

All writes are single-key. No multi-key transactions.

---

## 6. OpenTelemetry propagation

- All three services (orchestrator, A2A nodes, main-langgraph) initialize
  the OTel SDK at startup and export to `OTEL_EXPORTER_OTLP_ENDPOINT` over gRPC.
- `opentelemetry-instrumentation-fastapi` auto-instruments every HTTP server.
- `opentelemetry-instrumentation-httpx` auto-instruments every HTTP client.
- The AmazeGraph proxy creates one manual span per remote-node hop:
  - name: `amazegraph.invoke_remote`
  - attributes: `amaze.graph_id`, `amaze.node_name`, `amaze.run_id`, `amaze.trace_id`
- W3C `traceparent` is propagated automatically by the httpx instrumentation,
  which means the FastAPI server on the remote node receives a parented
  context and its inner spans share the trace ID.
- System tests assert trace presence by querying Jaeger's HTTP API:
  `GET http://jaeger:16686/api/traces?service=main-langgraph&tags={"amaze.run_id":"run-1"}`.

---

## 7. Idempotency and concurrency rules

- `POST /register/node` — last-write-wins on `(graph_id, node_name)`.
- `POST /register/graph` — last-write-wins on `graph_id`.
- `POST /runs/{id}/events` — append-only; XADD assigns monotonic IDs even
  under concurrent appenders.
- `DELETE /register/node` — idempotent; OK if key is absent.

---

## 8. Out of scope for Sprint 1

Anything not enumerated above is out of scope and MUST not be relied on by
remote nodes or the driver. Specifically:

Health checks/heartbeats; bearer tokens or any auth between components; per-node
read/write field policies; real LLM calls inside remote nodes; conditional
edges; `Send`; `Command(update, goto)`; `interrupt()`; checkpointers; multi-host
deployment; proxy / MCP / Envoy; UI / dashboards; retry policy on remote-node
failure; full `runtime` propagation; local callbacks across the wire; caching;
streaming responses (`POST /invoke` is request/response only).
