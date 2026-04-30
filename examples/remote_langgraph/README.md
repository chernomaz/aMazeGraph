# Remote LangGraph Demo

Three-node graph: one local node (`start`) plus two remote nodes (`research`,
`writer`) executed end-to-end via the orchestrator. Proves graph
registration, `remote_node` proxy invokes against A2A endpoints, run events
in Redis, and matching Jaeger traces.

## Run

```
docker compose -f docker/compose.remote-langgraph.yml up --build
```

The `main-langgraph` service exits 0 once `final_answer` is set on the
final state, after emitting `run-end`.

## Inspect

Jaeger UI: http://localhost:16686 — service `main-langgraph`. Look for the
`main-langgraph.run` parent span and child `amazegraph.invoke_remote` spans
for `research` and `writer`.

Orchestrator run state:

```
curl http://localhost:8001/runs/run-1 | jq
```

The response contains `meta.status` and the full event stream
(`run-start`, `node-enter`/`node-exit` per remote hop, `run-end`).
