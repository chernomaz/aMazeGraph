# AmazeGraph

AmazeGraph extends LangGraph with one additional capability: running selected graph nodes remotely.

Existing LangGraph nodes, edges, state, reducers, conditional routing, and compile/invoke flow stay the same.  
For local nodes, continue using `add_node(...)`.  
For distributed nodes, use `remote_node(...)`.  
No changes to node business logic. One graph-level change to opt into remote execution.

```python
from amaze_graph import AmazeGraph

graph = AmazeGraph(State)

graph.add_node("local_step", local_step)
graph.remote_node("remote_research", endpoint="research-service")

graph.add_edge("local_step", "remote_research")
graph.add_edge("remote_research", END)

app = graph.compile()
```

![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)
![License Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-green)
![LangGraph](https://img.shields.io/badge/LangGraph-0.2%2B-purple)

---

## What is aMazeGraph?

aMazeGraph enables distributed execution of [LangGraph](https://github.com/langchain-ai/langgraph) graphs: individual nodes run as standalone HTTP services on different hosts while the driver graph stays 100% native LangGraph (`StateGraph`, `add_edge`, `app.ainvoke`). A Redis-backed orchestrator handles service discovery, and every remote invocation is fully traced in Jaeger (OpenTelemetry) and LangSmith.

The only new author surface is **two primitives**:

| Primitive | Where | What it does |
|-----------|-------|--------------|
| `@remote_node(graph_id, node_name)` | remote service | Registers an async handler; `serve_node()` starts the FastAPI server |
| `builder.remote_node("name")` | driver | Declares a node as remote; the proxy resolves and calls it at runtime |

Everything else — reducers, `Command`, `Send`, parallel fan-out, checkpointing, conditional routing — works exactly as documented in LangGraph.

---

## Migrating Existing LangGraph Code

If you already have a working LangGraph graph, moving a node to a remote service takes three steps.

### Before (local node)

```python
from langgraph.graph import StateGraph, END
from typing import TypedDict

class MyState(TypedDict):
    query: str
    result: str

async def researcher(state: MyState) -> dict:
    return {"result": f"researched: {state['query']}"}

builder = StateGraph(MyState)
builder.add_node("researcher", researcher)
builder.set_entry_point("researcher")
builder.add_edge("researcher", END)
app = builder.compile()

result = await app.ainvoke({"query": "LangGraph remoting"})
```

### After — Step 1: extract the node into its own file

```python
# researcher_node.py  (runs on any host)
from sdk.amaze import remote_node, serve_node

@remote_node(graph_id="my_graph", node_name="researcher")
async def researcher_handler(state: dict, config: dict) -> dict:
    return {"result": f"researched: {state['query']}"}

if __name__ == "__main__":
    serve_node()   # auto-registers with orchestrator on startup
```

### After — Step 2: swap `StateGraph` → `AmazeGraph` in the driver

```python
from sdk.amaze import AmazeGraph
from typing import TypedDict

class MyState(TypedDict):
    query: str
    result: str

builder = AmazeGraph(MyState, graph_id="my_graph",
                     orchestrator_url="http://localhost:8011")
builder.remote_node("researcher")   # ← was: builder.add_node("researcher", fn)
builder.set_entry_point("researcher")
builder.add_edge("researcher", END)
app = builder.compile()

# run_id / trace_id are optional observability fields — pass them to get a
# run event log in the orchestrator and amaze.run_id on OTel spans.
# The graph executes correctly without them.
result = await app.ainvoke({"query": "LangGraph remoting",
                            "run_id": "run-1", "trace_id": "trace-1"})
```

### After — Step 3: start the services

See [Docker quick start](#docker-quick-start) or [running a node without Docker](#running-a-remote-node-without-docker).

### Migration reference

| LangGraph concept | aMazeGraph equivalent |
|---|---|
| `builder.add_node("n", fn)` — local | `builder.add_node("n", fn)` unchanged |
| `builder.add_node("n", fn)` — remote | `builder.remote_node("n")` + `@remote_node` in separate file |
| `Annotated[list, operator.add]` reducer | unchanged — reducers work across remote boundaries |
| `Command(update=…, goto=…)` | return `Command(…)` directly from handler |
| `Send("node", arg)` | return `Send(…)` directly from handler |
| Checkpointer | pass `checkpointer=AsyncRedisSaver(…)` to `AmazeGraph(…)` |
| LangSmith tracing | set env vars as usual — traces propagate automatically |

---

## Architecture

```text
                 register / heartbeat
Remote Node ─────────────────────────────▶ Orchestrator
  service                                  registry + run events
  /invoke                                  Redis + Jaeger

Driver ───── resolve node ───────────────▶ Orchestrator
Driver ───── HTTP POST /invoke ─────────▶ Remote Node
Driver ◀──── state patch / command ────── Remote Node
```

The orchestrator image embeds both Redis and Jaeger — **no separate infra containers needed**.

Remote nodes can run anywhere that can reach the orchestrator port. The driver calls each remote node directly via its registered endpoint; traffic does not pass through the orchestrator.

---

## Supported LangGraph Features

| Capability | Sprint |
|---|:---:|
| Read graph state / return partial state patch | S1 |
| Async node handlers | S1 |
| Static edges, entry/exit points | S1 |
| OTel distributed traces (Jaeger) | S1 |
| Run event stream (Redis Streams) | S1 |
| `Annotated` reducers (`operator.add`) | S2 |
| `MessagesState` / `add_messages` | S2 |
| Conditional routing (`add_conditional_edges`) | S2 |
| Parallel fan-out with reducer merge | S2 |
| Real LLM calls + MCP tool use inside remote nodes | S2 |
| Runtime context (`__amaze_runtime_context__`) | S2 |
| Opaque subgraph nodes (internal `StateGraph`) | S3 |
| Input / output / private state schemas | S3 |
| Recursion metadata (`langgraph_step`) | S3 |
| Richer error taxonomy (`node_error`, `proxy_block`, `timeout`) | S3 |
| `Command(update, goto)` — single target | S4 |
| `Command(goto=[…])` — parallel multi-target | S4 |
| `Send("node", arg)` — selective fan-out | S5 |
| `Command(update, goto=[Send(…)])` — combined | S5 |
| Thread persistence via `AsyncRedisSaver` checkpointer | S6 |
| Node-level TTL cache (Redis) | S7 |
| LangSmith trace propagation into remote nodes | S7 |

---

## Prerequisites

- **Docker + Docker Compose** — required for the orchestrator (infra always runs in Docker)
- **Python 3.11+** — for running remote nodes without Docker
- `OPENAI_API_KEY` — optional, only needed for LLM demos (`--profile sprint2`)
- `LANGSMITH_API_KEY` — optional, for LangSmith tracing

---

## Docker Quick Start

The orchestrator container bundles **Redis** and **Jaeger** — one `up` command starts everything.

### Infrastructure only (orchestrator + Redis + Jaeger)

Use this when setting up a new machine or when you want to run remote nodes separately:

```bash
docker compose -f docker/compose.remote-langgraph.yml up --build orchestrator
```

Verify it is ready:

```bash
curl http://localhost:8011/health
# {"status": "ok", "redis": "ok", "jaeger": "ok"}
```

### Full stack (orchestrator + sample nodes + demo driver)

```bash
git clone <repo-url>
cd aMazeGraph

# Start orchestrator (Redis + Jaeger) + two sample remote nodes + demo driver
docker compose -f docker/compose.remote-langgraph.yml up --build
```

Add sprint profiles to activate additional capabilities:

```bash
docker compose -f docker/compose.remote-langgraph.yml \
  --profile sprint2 \   # MCP tools + real LLM + parallel fan-out
  --profile sprint3 \   # subgraphs, schema splits, recursion
  --profile sprint4 \   # Command routing
  --profile sprint5 \   # Send fan-out + pipeline demo
  --profile sprint6 \   # thread persistence (checkpointer)
  --profile sprint7 \   # node-level TTL cache + LangSmith
  up --build
```

### Service URLs after startup

| URL | Description |
|-----|-------------|
| `http://localhost:8011/health` | Orchestrator health |
| `http://localhost:8011/runs/{run_id}` | Run event log (JSON) |
| `http://localhost:16696` | Jaeger trace UI |
| `http://localhost:9012/healthz` | `remote-research` node health |
| `http://localhost:9013/healthz` | `remote-writer` node health |

### Spreading nodes across hosts (Docker)

Set `AMAZE_NODE_PUBLIC_ENDPOINT` to the **externally reachable** address of each node container. The orchestrator stores this URL; the driver calls it directly.

```yaml
# docker-compose.yml on host B (IP 10.0.1.20)
services:
  remote-researcher:
    image: amazegraph-remote-node
    environment:
      AMAZE_NODE_PORT: "9002"
      AMAZE_NODE_PUBLIC_ENDPOINT: "http://10.0.1.20:9002/invoke"   # host B's address
      AMAZE_ORCHESTRATOR_URL:    "http://10.0.1.10:8011"          # host A's orchestrator
    ports:
      - "9002:9002"
```

No VPN or service mesh required — the driver on host A resolves the endpoint from the orchestrator and calls `http://10.0.1.20:9002/invoke` directly.

---

## Running a Remote Node Without Docker

The orchestrator (Redis + Jaeger) **always runs in Docker**. A remote node can run as a plain Python process on any host — same machine or a separate server — as long as it can reach the orchestrator's published port.

### Step 1 — Install dependencies on the worker host

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

### Step 2 — Start the remote node

**On the same machine as the Docker stack** (orchestrator published on `localhost:8011`):

```bash
export AMAZE_NODE_PORT=9002
export AMAZE_NODE_PUBLIC_ENDPOINT=http://localhost:9002/invoke
export AMAZE_ORCHESTRATOR_URL=http://localhost:8011
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317  # optional, Jaeger gRPC

python -m examples.remote_nodes.research_node
# Self-registers with the orchestrator; health check at :9002/healthz
```

**On a different host** (worker IP `10.0.1.20`, orchestrator on `10.0.1.10`):

```bash
export AMAZE_NODE_PORT=9002
# Must be reachable by the driver — do NOT use 'localhost' here
export AMAZE_NODE_PUBLIC_ENDPOINT=http://10.0.1.20:9002/invoke
export AMAZE_ORCHESTRATOR_URL=http://10.0.1.10:8011
export OTEL_EXPORTER_OTLP_ENDPOINT=http://10.0.1.10:4317  # optional

python -m examples.remote_nodes.research_node
```

The node POSTs its endpoint to the orchestrator on startup and removes it on shutdown. The driver resolves the endpoint at call time and invokes it directly.

### Step 3 — Run the driver (plain Python, any host)

```bash
export AMAZE_ORCHESTRATOR_URL=http://10.0.1.10:8011
python -m examples.remote_langgraph.main
```

### Creating a new node from scratch

A remote node is a plain async function decorated with `@remote_node`. A ready-to-use template is at `examples/remote_nodes/my_node.py`:

```python
from sdk.amaze import remote_node, serve_node

@remote_node(graph_id="my_graph", node_name="my_node")
async def my_node_handler(state: dict, config: dict) -> dict:
    result = f"processed: {state.get('input', '')}"
    return {"output": result}

if __name__ == "__main__":
    serve_node()
```

Run it without Docker (pick any free port, e.g. 9020):

```bash
export AMAZE_NODE_PORT=9020
export AMAZE_NODE_PUBLIC_ENDPOINT=http://localhost:9020/invoke
export AMAZE_ORCHESTRATOR_URL=http://localhost:8011

python -m examples.remote_nodes.my_node
```

Verify it registered:

```bash
curl http://localhost:9020/healthz
curl http://localhost:8011/resolve/node/my_graph/my_node
```

Copy `my_node.py`, rename it, change `GRAPH_ID` / `node_name`, and add your logic. Each node must use a unique port.

### Wiring the driver

In the driver, replace `StateGraph` with `AmazeGraph` and declare remote nodes with `builder.remote_node(...)`:

```python
from sdk.amaze import AmazeGraph

# graph_id must match GRAPH_ID in every remote node file
# sync=True  → use graph.stream() / graph.invoke() (plain Python)
# sync=False → use graph.astream() / graph.ainvoke() (asyncio, default)
builder = AmazeGraph(
    AgentState,
    graph_id="my_graph",
    orchestrator_url="http://localhost:8011",
    sync=True,
)

builder.remote_node("my_node")   # no local function — resolved via orchestrator

# All edges and conditional routing stay exactly the same as with StateGraph
builder.add_edge(START, "my_node")
builder.add_edge("my_node", END)

graph = builder.compile()

# sync=True: use the normal sync API
for step in graph.stream(initial_state, config=config, stream_mode="updates"):
    print(step)
```

**Key rules:**
- `graph_id` in the driver must match `GRAPH_ID` in the node file — this is how the orchestrator routes calls.
- The remote node handler must be `async def` — it runs inside a FastAPI server regardless of whether the driver uses `sync=True` or not.
- `sync=True` keeps your existing `graph.stream()` / `graph.invoke()` calls unchanged.
- `sync=False` (default) requires `graph.astream()` / `graph.ainvoke()` inside `asyncio.run()`.

---

## Defining a Remote Node

### Minimal example

```python
# my_node.py
from sdk.amaze import remote_node, serve_node

@remote_node(graph_id="my_graph", node_name="processor")
async def processor_handler(state: dict, config: dict) -> dict:
    return {"output": state["input"].upper()}

if __name__ == "__main__":
    serve_node()
```

```bash
AMAZE_NODE_PORT=9002 \
AMAZE_NODE_PUBLIC_ENDPOINT=http://localhost:9002/invoke \
AMAZE_ORCHESTRATOR_URL=http://localhost:8011 \
python my_node.py
```

### Dynamic routing with `Command`

```python
from langgraph.types import Command
from sdk.amaze import remote_node

@remote_node(graph_id="my_graph", node_name="router")
async def router_handler(state: dict, config: dict) -> Command:
    target = "fast_path" if state.get("priority") == "high" else "slow_path"
    return Command(update={"routed_to": target}, goto=target)
```

### Selective fan-out with `Send`

```python
from langgraph.types import Send
from sdk.amaze import remote_node

@remote_node(graph_id="my_graph", node_name="dispatcher")
async def dispatcher_handler(state: dict, config: dict):
    items = state.get("items", [])
    return [Send("worker", {"item": item}) for item in items]
```

### Node-level TTL cache

```python
@remote_node(graph_id="my_graph", node_name="expensive_node", cache_ttl=60)
async def expensive_handler(state: dict, config: dict) -> dict:
    # Repeated calls with the same state within 60 s return the cached response.
    # The remote node is not called; no run events are emitted for cache hits.
    return {"result": run_expensive_computation(state["input"])}
```

### Thread persistence (multi-turn conversations)

```python
from langgraph.checkpoint.redis.aio import AsyncRedisSaver
from sdk.amaze import AmazeGraph

checkpointer = AsyncRedisSaver(redis_url="redis://localhost:6380")
await checkpointer.asetup()

builder = AmazeGraph(MyState, graph_id="my_graph", checkpointer=checkpointer)
builder.remote_node("accumulator")
# ... edges ...
app = builder.compile()

cfg = {"configurable": {"thread_id": "session-42"}}

result1 = await app.ainvoke({"input": "turn-1", "run_id": "r1", "trace_id": "t1"}, config=cfg)
result2 = await app.ainvoke({"input": "turn-2", "run_id": "r2", "trace_id": "t2"}, config=cfg)
# State from turn-1 is restored from Redis before turn-2 executes.
```

---

## LangSmith Integration

LangSmith tracing works exactly as in stock LangGraph — set the usual env vars and nothing else:

```bash
export LANGCHAIN_TRACING_V2=true
export LANGSMITH_API_KEY=ls__...
export LANGCHAIN_PROJECT=aMazeGraph
```

aMazeGraph propagates the driver's LangSmith parent run ID to each remote node over the wire. Inside the remote node, `serve_node()` reconstructs a `LangChainTracer` before calling the handler. As a result:

- LLM calls inside remote nodes appear **nested under the graph root** in LangSmith
- Each remote invocation has its own child span in the trace tree
- The complete graph trace — local nodes and remote nodes — is visible in a single LangSmith run

---

## Observability

### Jaeger (distributed traces)

| Mode | Jaeger UI |
|------|-----------|
| Docker compose | `http://localhost:16696` |
| Standalone orchestrator | `http://localhost:16686` |

Every remote invocation produces an OTel span tagged with `amaze.graph_id`, `amaze.node_name`, `amaze.run_id`, and `amaze.cache_hit`.

### Run event stream (Redis)

```bash
curl http://localhost:8011/runs/{run_id} | jq
```

Events in order: `run-start` → `node-enter` → `node-exit` (or `node-error`) → `run-end`.

```json
{
  "meta": { "status": "done", "graph_id": "my_graph", "trace_id": "trace-1" },
  "events": [
    { "event": "run-start",  "ts": "2026-05-05T10:00:00Z" },
    { "event": "node-enter", "node_name": "researcher",    "ts": "..." },
    { "event": "node-exit",  "node_name": "researcher",    "status": "ok", "ts": "..." },
    { "event": "run-end",    "status": "done",             "ts": "..." }
  ]
}
```

---

## Environment Variables

### Driver (`AmazeGraph`)

| Variable | Default | Description |
|---|---|---|
| `AMAZE_ORCHESTRATOR_URL` | `http://localhost:8001` | Orchestrator base URL |
| `AMAZE_NODE_INVOKE_TIMEOUT` | `30` | Remote node HTTP timeout (seconds) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | _(none)_ | Jaeger gRPC endpoint; OTel disabled if absent |

### Remote node (`serve_node`)

| Variable | Default | Description |
|---|---|---|
| `AMAZE_NODE_PORT` | **required** | Port the node listens on |
| `AMAZE_NODE_HOST` | `0.0.0.0` | Bind address |
| `AMAZE_NODE_PUBLIC_ENDPOINT` | `http://<host>:<port>/invoke` | URL advertised to the orchestrator |
| `AMAZE_ORCHESTRATOR_URL` | `http://localhost:8001` | Orchestrator base URL |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | _(none)_ | Jaeger gRPC endpoint |
| `LANGCHAIN_TRACING_V2` | `false` | Enable LangSmith tracing |
| `LANGSMITH_API_KEY` | _(none)_ | LangSmith API key |
| `LANGCHAIN_PROJECT` | `aMazeGraph` | LangSmith project name |
| `LOG_LEVEL` | `INFO` | Logging level |

---

## Orchestrator HTTP API

The orchestrator is auto-managed by the SDK; you rarely call it directly. Key endpoints for debugging:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check (`{"status":"ok","redis":"ok"}`) |
| `POST` | `/register/node` | Register a remote node (called by `serve_node()`) |
| `DELETE` | `/register/node` | Unregister a node (called on shutdown) |
| `GET` | `/resolve/node/{graph_id}/{node_name}` | Look up a node's endpoint |
| `POST` | `/register/graph` | Store graph topology (called by `builder.compile()`) |
| `GET` | `/runs/{run_id}` | Fetch run metadata + ordered event log |
| `POST` | `/runs/{run_id}/events` | Append a run event |
| `GET` | `/cache/{key}` | Cache lookup |
| `PUT` | `/cache/{key}` | Cache store |

---

## Docker Compose Profiles

| Profile flag | Additional services activated |
|---|---|
| _(none)_ | orchestrator + Redis + Jaeger, `remote-research`, `remote-writer`, `main-langgraph` |
| `--profile sprint2` | MCP server, LLM/tool node, parallel fan-out branches |
| `--profile sprint3` | Subgraph node, counter node, schema node |
| `--profile sprint4` | Command routing node |
| `--profile sprint5` | Send dispatcher, pipeline demo |
| `--profile sprint6` | Accumulator node (thread persistence demo) |
| `--profile sprint7` | Cached node (TTL cache demo) |

---

## Project Structure

```
aMazeGraph/
├── sdk/amaze/
│   ├── langgraph.py         # AmazeGraph class + OrchestratorClient
│   ├── node.py              # @remote_node decorator + serve_node()
│   └── _messages.py         # LangChain message serialization helpers
├── services/
│   └── orchestrator/
│       └── main.py          # FastAPI orchestrator (node registry + event stream)
├── examples/
│   ├── remote_langgraph/
│   │   └── main.py          # Multi-scenario demo driver (22+ scenarios)
│   └── remote_nodes/           # Sample remote node handlers
│       ├── research_node.py
│       ├── writer_node.py
│       ├── command_node.py
│       ├── send_node.py
│       ├── accumulator_node.py
│       ├── cached_node.py
│       └── ...
├── docker/
│   ├── Dockerfile.orchestrator
│   ├── Dockerfile.remote-node
│   ├── Dockerfile.main
│   └── compose.remote-langgraph.yml
├── docs/
│   └── remote-langgraph-contract.md  # HTTP wire contract specification
├── tests/
│   ├── conftest.py           # pytest fixtures (compose stack management)
│   ├── system/
│   │   ├── test_sprint2.py   # ST-RLG-7..14
│   │   ├── test_sprint3.py   # ST-RLG-15..18
│   │   ├── test_sprint4.py   # ST-RLG-19..22
│   │   ├── test_sprint5.py   # ST-RLG-23..26
│   │   ├── test_sprint6.py   # ST-RLG-27
│   │   └── test_sprint7.py   # ST-RLG-28..31
│   └── test_sprint1.py       # ST-RLG-1..6
├── requirements.txt
├── LICENSE
└── README.md
```

---

## Limitations

aMazeGraph is currently a distributed execution prototype, not a production service mesh.

Known limitations:

- remote nodes are invoked over HTTP directly by the driver
- node registration currently assumes trusted network access
- no built-in mTLS or node identity verification yet
- remote node side effects must be idempotent if retries/resume are enabled
- streaming and LangGraph `interrupt()` are planned but not stable yet
- embedded Redis and Jaeger are for local/demo use, not production deployment

---

## License

Copyright 2026 Arseniy Chernomaz

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full text.
