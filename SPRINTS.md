# SPRINTS

## Sprint 1 — Remote LangGraph Nodes via A2A + Orchestrator

**Goal:** Prove that a normal LangGraph program can execute two remote nodes
through A2A nodes registered in the orchestrator, while keeping LangGraph API
changes minimal. Demo prints `final_answer` produced end-to-end via
`docker compose -f docker/compose.remote-langgraph.yml up --build`.

### Status legend
☐ pending · ◐ in progress · ✓ done

### Sprint 1 tasks

| ID | Owner | Task | Deliverable | Status |
|---|---|---|---|---|
| T0 | Architect | Tracking-file scaffolding | SPRINTS.md, Progress.md, requirements.txt, .env.example | ✓ |
| T1 | Architect | Remote-node contract document | docs/remote-langgraph-contract.md | ✓ |
| T2 | Dev | Orchestrator service | services/orchestrator/main.py | ✓ |
| T3 | Dev | AmazeGraph SDK | sdk/amaze/langgraph.py | ✓ |
| T4 | Dev | Research A2A node | examples/a2a_nodes/research_node.py | ✓ |
| T5 | Dev | Writer A2A node | examples/a2a_nodes/writer_node.py | ✓ |
| T6 | Dev | Main demo program | examples/remote_langgraph/main.py | ✓ |
| T7 | DevOps | Compose stack + Dockerfiles | docker/compose.remote-langgraph.yml + Dockerfiles | ✓ |
| T8 | QA | System tests | tests/system/test_remote_langgraph.py | ✓ |

### Demo verification (2026-04-29)

End-to-end demo executed successfully. `docker compose ... run --rm main-langgraph` produces `final_answer`. Run timeline visible at `GET http://localhost:8011/runs/run-1`. Distributed trace (51 spans) visible at `http://localhost:16696` for service `main-langgraph`.

### Test results (2026-04-30 morning)

All 6 system tests pass: `6 passed in 21.91s`. ST-RLG-1..6 green after fixture rework (ST-RLG-5 now uses orchestrator unregister API instead of `docker stop`; ST-RLG-6 starts a sidecar debug container with last-write-wins re-registration). Code-review 🟡 items also applied (path-param regex, endpoint URL validation, atomic run-end via Lua/cjson, RunnableConfig typing, httpx.TransportError catch, response_model WHY comment).

Sprint 1 Definition of Done: **all items checked**.

### Post-S1 polish (2026-04-30 afternoon)

Sprint 1 was already DoD-complete. The following changes are quality-of-life improvements made before Sprint 2 starts — none of them alter the contract or system test list, all 6 ST-RLG tests still pass after each step.

| Change | Files | Why |
|---|---|---|
| Embedded Redis in orchestrator container | `docker/Dockerfile.orchestrator`, `docker/orchestrator-entrypoint.sh`, `docker/compose.remote-langgraph.yml` | Eliminates a class of failures where two compose stacks on the same host fight for redis port 6380 and one container ends up network-orphaned. "Meanwhile" arrangement — re-extract before production. |
| Embedded Jaeger all-in-one in orchestrator | same files | Same rationale + simpler dev stack. Jaeger binary copied via multi-stage `FROM jaegertracing/all-in-one:1.57 AS jaeger`. |
| Trimmed host-port publishing | compose | Only ports the developer actually needs are published: orchestrator `8011→8001`, Jaeger UI `16696→16686`, a2a-research `9012→9002`, a2a-writer `9013→9003`. Redis stays internal-only. |
| stdlib `logging` everywhere | `examples/a2a_nodes/_common.py`, `research_node.py`, `writer_node.py`, `examples/remote_langgraph/main.py`, `services/orchestrator/main.py` | Replaced 16 `print()` calls with `logger.info/warning/error`. Unified format `<ts> [<LEVEL>] <service> <logger>: <msg>` across all containers — including uvicorn's startup/access/error logs (via `uvicorn.run(log_config=None)` and `python -m` entrypoint for orchestrator). httpx logger comes through automatically. |
| Healthcheck noise filter | both `setup_logging` helpers | Drops `GET /healthz` and `GET /health ` lines from `uvicorn.access`. External callers (pytest's ST-RLG-1) still see their own assertions; only docker's 2-second internal pings are silenced. |
| 5-node demo graph | `examples/remote_langgraph/main.py`, `tests/system/test_remote_langgraph.py` | Added `post_research` and `post_writer` **local** nodes between/after the remote ones. They each log a one-liner and append to a `log_trail` list in state, demonstrating mixed local + remote orchestration. ST-RLG-3 updated to assert the new edge structure. |

**Compose service count:** 5 → **3** long-running (`orchestrator`, `a2a-research`, `a2a-writer`) + 1 one-shot `main-langgraph`.

**Demo graph topology:** `start (local) → research (remote) → post_research (local) → writer (remote) → post_writer (local) → END`

### Parallel phases

- **P1 (sequential):** T0 → T1
- **P2 (parallel):** T2, T3
- **P3 (parallel):** T4, T5, T6
- **P4 (sequential):** T7 → T8

### Agreed system tests (sign-off pending)

Each test runs against the live compose stack and verifies (1) HTTP/exit code,
(2) Redis Stream entries, (3) Jaeger trace.

| ID | Scenario | Pass condition |
|---|---|---|
| ST-RLG-1 | Orchestrator health | `GET /health` 200; redis ping ok; service span in Jaeger |
| ST-RLG-2 | Both A2A nodes register | `GET /resolve/node/...` returns endpoints; Redis keys present |
| ST-RLG-3 | Graph manifest registers on `compile()` | `GET /graphs/demo_graph_v1` lists 3 nodes + 3 edges |
| ST-RLG-4 | Full e2e run | demo exits 0; final state has `final_answer`; XRANGE shows ordered events; Jaeger trace has spans for all three nodes with shared trace_id |
| ST-RLG-5 | Missing remote node | writer absent → demo exits non-zero with `RemoteNodeNotRegistered`; `node-error` event in stream |
| ST-RLG-6 | Invalid remote response | research returns non-dict patch → demo exits non-zero with `InvalidStatePatch`; `node-error` event |

### Definition of Done

- Two A2A nodes register with orchestrator.
- Main program runs LangGraph with 3 nodes (1 local, 2 remote).
- `workflow.remote_node(...)` is the only new LangGraph API surface.
- Generated proxy resolves graph_id+node_name through orchestrator and POSTs.
- Remote A2A node returns dict `state_patch`; LangGraph merges and continues.
- `docker compose ... up --build` works in one command.
- All 6 system tests pass.
- Progress.md updated with timing table.
- SPRINTS.md task statuses updated.

### Out of scope (deferred)

Health checks/heartbeats, auth tokens, per-node read/write policies, real LLM
calls, conditional edges, `Send`, `Command`, `interrupt`, checkpointing,
multi-host deployment, proxy/MCP, UI, retry policies, `runtime` propagation,
local callbacks across the wire.
