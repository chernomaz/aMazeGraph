# Remote LangGraph Node Execution — Contract

Sprint 1 + Sprint 2 addenda. Sprint 2 sections live under § 9 "Sprint 2
addendum" so the original Sprint 1 contract remains the binding floor.

This document is the binding contract between the four moving parts:

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

---

## 9. Sprint 2 addendum

Sprint 2 adds support for 13 of the 28 LangGraph node capabilities tracked in
`Features.md`. None of the additions break Sprint 1 clients — all wire changes
are additive (new optional request fields, new response sibling keys are not
introduced in Sprint 2; reducer behavior is local to the driver).

### 9.1 State-schema reducers

LangGraph reducers run **locally** inside the driver when a returned
`state_patch` is merged into the channel. Remote nodes do not know about
reducers — they just return their patch.

The contract obligation falls on the **state schema author**:

> **Rule.** Any field that two or more concurrent branches can write **MUST**
> be declared with a reducer in the state schema (`Annotated[Type,
> reducer]`). Fields without a reducer are last-write-wins, and "last" is
> non-deterministic under parallel execution.

Reducers shipped / commonly used:

- **None** (plain `Type`) — overwrite (default).
- **`operator.add`** — concatenation/sum for `list`, `tuple`, `int`, `float`,
  `str`. Does **not** work for `dict` (no `+` operator on dicts).
- **`langgraph.graph.message.add_messages`** — message-history merge with
  dedup-by-id; accepts dict-form messages and converts to `BaseMessage`;
  honors `RemoveMessage(id=...)` sentinels.
- **Custom callable** `(current, update) -> new` — must be commutative and
  associative if used under parallel writes.

**Sprint 2 demo schema** (`examples/remote_langgraph/main.py`):

```python
from typing import Annotated, TypedDict
import operator
from langgraph.graph.message import add_messages
from langchain_core.messages import BaseMessage

class GraphState(TypedDict, total=False):
    run_id: str
    trace_id: str
    request: str
    research_result: str
    final_answer: str
    log_trail:  Annotated[list[str], operator.add]      # append, never overwrite
    results:    Annotated[list[str], operator.add]      # parallel-fan-out target
    messages:   Annotated[list[BaseMessage], add_messages]  # MessagesState semantics
    mode: str
    echoed_thread: str
    echoed_tenant: str
```

### 9.2 Message serialization across the wire

`BaseMessage` instances are not JSON-serializable as-is. The SDK provides
`sdk/amaze/_messages.py` with two helpers:

```python
def serialize_messages(messages: list[BaseMessage]) -> list[dict]: ...
def deserialize_messages(items: list[dict]) -> list[BaseMessage]: ...
```

**Wire shape** (one message):
```json
{ "role": "human|assistant|system|tool",
  "content": "...",
  "id": "msg_abc",
  "tool_calls": [...],     // assistant only, optional
  "tool_call_id": "...",   // tool only
  "additional_kwargs": {}, // optional pass-through
  "response_metadata": {}  // optional pass-through
}
```

Conventions:
- The proxy serializes `state["messages"]` (and any other `BaseMessage`-typed
  field declared in the state schema) before POSTing. The remote `serve_node`
  deserializes when the handler is annotated to receive `BaseMessage` objects;
  otherwise leaves the dicts as-is.
- The remote node's returned `state_patch` may include either dict-form or
  `BaseMessage`-form messages; both work because `add_messages` accepts
  either. The proxy normalizes to dict-form before returning.
- Message `id`s are preserved end-to-end so `add_messages` dedup works.

### 9.3 `runtime_context` field

A new optional top-level field on the `/invoke` request body:

```json
{
  "graph_id": "...",
  "node_name": "...",
  "run_id": "...",
  "trace_id": "...",
  "state": { ... },
  "config": { ... },
  "runtime_context": {
    "tenant_id": "acme",
    "model_name": "gpt-4o-mini"
  }
}
```

Rules:
- `runtime_context` is a JSON object. Keys and values must be
  JSON-serializable (no live Python objects, no DB handles, no callable
  references).
- The driver sources `runtime_context` from `RunnableConfig.configurable`
  under the dedicated key `__amaze_runtime_context__` (so it doesn't collide
  with general `configurable` keys). Authors who want to set it call the
  forthcoming SDK helper `AmazeGraph.with_runtime_context({...})` (T4) or pass
  it via `app.invoke(state, config={"configurable": {"__amaze_runtime_context__": {...}}})`.
- On the remote side, `serve_node` reconstructs a stub `Runtime` exposing
  `runtime.context` as a dataclass-like attribute namespace. Access to
  `runtime.store` or `runtime.stream_writer` raises a clear
  `RuntimeNotAvailable` error directing authors to the relevant Hard-tier
  capabilities (#22 store, #24 streaming).
- Older clients omitting `runtime_context` → server treats it as `{}`.
  Backwards-compatible.

### 9.4 Conditional routing with remote targets

LangGraph's `add_conditional_edges` runs the routing function locally in the
driver — there is no wire change. The router decides which node to invoke
next; if that node is a `remote_node`, the existing `/invoke` flow handles it.

Author obligations:
- The router function must return a node name (or list of names for
  parallel-fan-out via conditional edges) that exists in the graph manifest.
- The orchestrator **does not** validate router outputs in Sprint 2 — that
  belongs to capability #14 (`Command(goto)`) in a future sprint.

Example:
```python
def route_by_mode(state: GraphState) -> str:
    return "researcher_a" if state["mode"] == "fast" else "researcher_b"

graph.add_conditional_edges(
    "planner",
    route_by_mode,
    {"researcher_a": "researcher_a", "researcher_b": "researcher_b"},
)
```

### 9.5 Parallel fan-out (real concurrency)

When a node has multiple outgoing static edges to remote nodes, LangGraph
fires both branches in the same superstep. The driver invokes them
**concurrently** via `asyncio.gather(...)`-equivalent behavior inside the
LangGraph runtime; the AmazeGraph proxy does not need to do anything special
beyond ensuring the httpx client pool can serve concurrent connections.

Operational notes:
- The `OrchestratorClient` httpx connection pool defaults to 100 concurrent
  connections (httpx default), which is sufficient for any reasonable fan-out
  width in Sprint 2.
- Concurrent `XADD run:{run_id}:events` calls are atomic per call and assigned
  monotonic Stream IDs by Redis. Event ordering inside a superstep is
  arrival-order, not declaration-order — tests must not assume otherwise.
- Wall-clock timestamps in `node-enter` events for sibling branches will
  **overlap**. ST-RLG-13 asserts this overlap as the proof of concurrency.
- State schema **must** declare a reducer (§9.1) for any field two siblings
  write, otherwise one patch silently overwrites the other.

### 9.6 LLM and tool calls inside remote nodes (Option A)

Sprint 2 locks the **observability-only** posture for cases #5 and #6: the
distributed runtime does not intercept, wrap, or enforce LLM/tool calls
beyond what stock LangGraph itself provides.

What the runtime guarantees:
- OTel `traceparent` is propagated to the remote node (already true in S1).
  LangChain's auto-instrumentation creates child spans for `BaseChatModel`
  and `BaseTool` calls; they nest correctly under the `amazegraph.invoke_remote`
  span in Jaeger.
- If both the driver and the remote node set
  `LANGCHAIN_TRACING_V2=true` and the same `LANGCHAIN_PROJECT`, LangSmith
  shows one merged run tree.

What the runtime does **NOT** guarantee:
- Token / dollar budgets.
- Tool allowlists.
- PII / DLP filtering of prompts.
- Audit log of LLM/tool calls in our Redis Stream (the stream only sees
  `node-enter` / `node-exit`).

Authors who need any of those must add them inside the remote node code or
via an external proxy (LiteLLM, etc.).

### 9.7 MCP integration

Sprint 2 vendors a FastMCP server at `examples/mcp_server/` (copied from
`/home/ubuntu/data/cloude/newAmazeControlPlane/aMaze/examples/mcp_server/`).

- The MCP server runs as a separate compose service named `mcp`,
  transport `streamable-http`, port 8000 internal.
- It auto-discovers LangChain `@tool` functions from `examples/mcp_server/tools/*.py`.
- Sprint 2's `examples/a2a_nodes/llm_tool_node.py` connects to the MCP
  server via `langchain-mcp-adapters` (or direct `fastmcp.Client`), binds
  the discovered tools to a `ChatOpenAI` instance, and lets the model
  decide which tool to call.
- The MCP server is independent of the orchestrator — it does not register
  with `/register/node`. It's a tool host, not a graph node.
- Calls to external services (OpenAI, Tavily) require `OPENAI_API_KEY` and
  `TAVILY_API_KEY` in the environment. The compose file passes them through
  from the host's `.env`. Tests that depend on these keys SKIP with a clear
  message when keys are absent.

### 9.8 Updated request schema (`POST /invoke`)


```json
{
  "graph_id": "...",
  "node_name": "...",
  "run_id": "...",
  "trace_id": "...",
  "state": { ... },                                  // may contain serialized messages
  "config": {
    "tags": [...],
    "metadata": { ... },
    "configurable": { ... },
    "run_name": null,
    "recursion_limit": 25
  },
  "runtime_context": { ... }                         // NEW in S2; optional
}
```

Response shape unchanged from Sprint 1 (`{"state_patch": dict}`). Sprint 2
clients **continue to** ignore unknown keys in the response, in preparation
for `command` / `interrupt` siblings in S4+.

---

## 10. Sprint 3 addendum

Sprint 3 adds support for 5 medium-tier capabilities (cases 19, 20, 25, 27, 28).
All changes are **additive** — Sprint 1 and Sprint 2 clients are unaffected.

### 10.1 Subgraph as opaque node (cases 19 + 20)

A remote node may internally compile and run its own `StateGraph`. The driver
sees only a single registered endpoint and a normal `state_patch` response.

Author obligations:
- Register the subgraph node under a single `node_name` (e.g. `"subgraph"`).
- The remote handler creates a `StateGraph`, compiles it locally (no
  orchestrator involvement), runs `await app.ainvoke(...)` inside the `async`
  handler, and returns the merged result as a plain dict.
- The inner graph's nodes and edges are invisible to the outer orchestrator;
  they do **not** appear in the outer graph manifest.
- Case #20 ("call subgraph manually") is the same wire contract — the handler
  decides internally whether to use LangGraph's `ainvoke` or call sub-nodes
  imperatively. The outer driver is unaffected.

No proxy or orchestrator changes required.

### 10.2 Recursion / step metadata (case 25)

`config["metadata"]["langgraph_step"]` and `config["recursion_limit"]` already
survive the wire via the S1/S2 `config` subset (§3.1). Sprint 3 verifies them
end-to-end and adds a guard against runaway recursion.

Wire guarantee (unchanged):
- `config.metadata.langgraph_step` — integer, incremented by LangGraph per
  superstep. Arrives at the remote node on every invocation. Remote nodes
  **may** read it for observability; they **must not** rely on it for
  correctness logic.
- `config.recursion_limit` — integer, the graph-level cap. LangGraph on the
  driver side raises `GraphRecursionError` when the limit is exceeded; the
  remote node is never invoked past that point.

Author obligations:
- Graphs with cycles where a remote node is inside the loop must declare an
  appropriate `recursion_limit` (default 25). A low limit (e.g. 3) is useful
  for test scenarios.
- The remote node handler may log `langgraph_step` for debugging.

### 10.3 Richer error taxonomy (case 27)

Sprint 3 extends the `node-error` run event with an `error_kind` field that
classifies the failure.

#### Updated run-event schema

The `error_kind` field is **new** in Sprint 3. Older consumers that don't
recognize it MUST ignore it (already required by §3.1 "future sprints may add
sibling keys").

`POST /runs/{run_id}/events` accepts the new optional field:

```json
{
  "event": "node-error",
  "graph_id": "...",
  "node_name": "...",
  "trace_id": "...",
  "status": "error",
  "error": "...",
  "error_kind": "node_error",
  "ts": "..."
}
```

`GET /runs/{run_id}` returns `error_kind` in the event object when present.

#### `error_kind` value taxonomy

| Value | Meaning | Proxy condition |
|---|---|---|
| `node_error` | The node handler raised an exception or returned malformed data | HTTP 5xx from node; non-dict `state_patch`; invalid JSON body |
| `proxy_block` | The proxy could not reach the node at all | `RemoteNodeNotRegistered` (404 from orchestrator); connection refused |
| `timeout` | The node took longer than the configured invoke timeout | `httpx.TimeoutException` during POST to node |
| `policy_violation` | Blocked by a future enforcement layer | *Reserved — not emitted in Sprint 3* |
| `tool_error` | A tool call inside the node failed | *Reserved — not emitted in Sprint 3* |

#### Proxy timeout configuration

The AmazeGraph proxy respects the `AMAZE_NODE_INVOKE_TIMEOUT` environment
variable (float, seconds, default `30.0`). Set it low in tests that need to
trigger a `timeout` error kind.

### 10.4 Input / output / private schemas (case 28)

LangGraph's `StateGraph(FullState, input=InputState, output=OutputState)`
mechanism works transparently with remote nodes.

Rules:
- `app.ainvoke(input_dict)` accepts only `InputState`-typed keys at the
  graph boundary; unrecognized input keys are ignored by LangGraph.
- The proxy sends the **full current channel state** to each remote node
  (same as Sprint 1). The remote node may read any field that exists in the
  channel at invocation time — including fields that are not part of
  `InputState` if prior nodes have populated them.
- `app.ainvoke(...)` returns only `OutputState`-typed keys. `PrivateState`
  fields are **not** present in the caller-facing result.
- Remote nodes whose `state_patch` writes to `PrivateState` fields function
  correctly; those fields are visible inside the graph but filtered from the
  final output.

No proxy or orchestrator changes required.

---

## 11. Sprint 4 addendum — `Command` response sibling (cases 14 + 15)

Sprint 4 lets a remote node drive graph routing instead of static edges by
returning an `AmazeCommand` from its handler. All validation is proxy-side;
the orchestrator does not change.

### 11.1 Handler-side authoring (`AmazeCommand`)

Remote node handlers that want to control routing return an `AmazeCommand`
instead of a plain dict:

```python
from sdk.amaze import AmazeCommand

# Case 14 — single goto
async def my_node(state, config):
    return AmazeCommand(update={"result": "done"}, goto="next_node")

# Case 15 — multi-goto (parallel fan-out)
async def my_node(state, config):
    return AmazeCommand(update={}, goto=["branch_a", "branch_b"])
```

`AmazeCommand` is a dataclass with two fields:
- `update: dict | None` — state fields to merge (equivalent to a `state_patch`).
  Defaults to `{}` when `None`.
- `goto: str | list[str]` — target node name(s).

Returning a plain dict always produces a normal `state_patch` — handlers can
still have a state field named `"command"` without any ambiguity.

### 11.2 Wire format (`POST /invoke` response)

`_common.py` translates an `AmazeCommand` return value into the wire response:

```json
{
  "command": {
    "update": { "result": "done" },
    "goto": "next_node"
  }
}
```

For multi-goto (Case 15):
```json
{
  "command": {
    "update": {},
    "goto": ["branch_a", "branch_b"]
  }
}
```

Rules:
- `command` is an optional top-level sibling of `state_patch`. When present,
  `state_patch` MUST NOT also be present (the proxy ignores any `state_patch`
  when `command` is detected).
- `command.update` is a JSON object. Defaults to `{}` when absent.
- `command.goto` is required and non-empty. Either a string (single target) or
  a non-empty array of strings (multi-target fan-out).
- Older clients that only read `state_patch` will see `None` and raise
  `InvalidStatePatch`. Sprint 1–3 clients **must** upgrade before routing to
  nodes that may return `AmazeCommand`.

### 11.3 Proxy behaviour

On receiving a response with a `command` key, the proxy:

1. Validates `command` shape (must be a dict). Emits `node-error`
   (`error_kind="proxy_block"`) and raises `InvalidCommand` on failure.
2. Normalises `goto` to a list. Rejects empty list.
3. Validates each `goto` target against
   `set(self._nodes) | {"__end__"}` — the set of node names registered with
   this `AmazeGraph` instance plus the LangGraph end sentinel. Unknown targets
   → `node-error` (`error_kind="proxy_block"`) + `InvalidCommand`.
4. Emits `node-exit` (status=ok).
5. Returns `langgraph.types.Command(update=..., goto=...)` to the driver.
   LangGraph then routes to the specified node(s) in the next superstep.

When `command` is absent the proxy falls through to the existing `state_patch`
validation path — fully backward-compatible.

### 11.4 `InvalidCommand` exception

```python
class InvalidCommand(AmazeGraphError):
    graph_id: str
    node_name: str
    reason: str   # human-readable, includes the offending goto value
```

Exported from `sdk.amaze`. Raised (and always preceded by a `node-error` run
event with `error_kind="proxy_block"`) when:
- `command` value is not a dict.
- `command.goto` is absent, empty, or not a str/list.
- Any `goto` target is not in `set(self._nodes) | {"__end__"}`.

### 11.5 Parallel fan-out via `Command.goto` list (Case 15)

`Command(goto=["a", "b"])` in LangGraph 1.1.6 dispatches both targets in the
**same superstep** — true parallel fan-out. Verified in Sprint 4 integration
test. State fields written by both branches must declare a reducer (§9.1),
identical to the static-edge fan-out constraint.

### 11.6 Orchestrator changes

None. The orchestrator does not inspect `command` or validate `goto` targets.
All routing validation is local to the proxy process, which has full knowledge
of the compiled graph's node set.
