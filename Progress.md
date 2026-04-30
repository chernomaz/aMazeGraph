# Progress

## Sprint 1 — Remote LangGraph Nodes via A2A + Orchestrator

### Activity log

- **2026-04-29** — Sprint 1 started.
  - Plan finalized and approved (saved at `/home/ubuntu/.claude/plans/i-want-to-enhance-tranquil-moler.md`).
  - User-decided constraints: OTel + Jaeger in S1, no TTL on registrations (explicit unregister), Redis Streams for events, `AMAZE_ORCHESTRATOR_URL` env var with constructor override.
  - All 8 tasks (T0–T8) implemented.
  - One-command demo executed successfully end-to-end.
  - System tests written but **not yet run** (pytest invocation interrupted by user).

- **2026-04-30 morning** — Sprint 1 closeout.
  - Pytest first pass: 4/6 (ST-RLG-5/6 fixture bugs, not production bugs).
  - Code review on 6 source files: 0 🔴, 5 🟡, 7 🟢.
  - All 5 🟡 items applied: path-param regex, endpoint URL validation, atomic run-end via Lua/cjson, `RunnableConfig` typing, `httpx.TransportError` catch, `response_model` WHY comment.
  - Conftest fixtures rewritten (ST-RLG-5 uses unregister API; ST-RLG-6 launches sidecar debug container).
  - Pytest second pass: **6/6 green in 21.91s**. Sprint 1 DoD complete.

- **2026-04-30 afternoon** — Post-S1 polish.
  - **Embedded Redis** in the orchestrator container (`redis-server --daemonize` in entrypoint script before `uvicorn`). Removed the `redis` compose service. Uses ephemeral in-memory store (`--save "" --appendonly no`) — re-extract before production.
  - **Embedded Jaeger all-in-one** in the orchestrator container (multi-stage Dockerfile copies `/go/bin/all-in-one-linux` from `jaegertracing/all-in-one:1.57`). Removed the `jaeger` compose service. Same "meanwhile" caveat.
  - **Trimmed host ports** to the minimum needed: 8011 (orchestrator API), 9012/9013 (a2a node `/healthz` for tests), 16696 (Jaeger UI). Redis 6380 and Jaeger OTLP 4337 / health 14279 no longer published.
  - **stdlib logging** migration. Replaced 16 `print()` calls with `logger.info/warning/error` across `_common.py`, `research_node.py`, `writer_node.py`, `main.py`, plus a `_setup_logging("orchestrator")` in `services/orchestrator/main.py`. Switched orchestrator entrypoint from `uvicorn …` CLI to `python -m services.orchestrator.main`, plus passed `log_config=None` to `uvicorn.run` so uvicorn's own loggers propagate to root and inherit our format. httpx logs come through for free.
  - **Healthcheck filter** on `uvicorn.access` logger silences the 2-second-interval `GET /healthz` / `GET /health` pings without affecting external traffic.
  - **5-node graph** in the demo: added `post_research` and `post_writer` local nodes between/after the remote ones. They log a one-liner and append to a `log_trail` list in state. ST-RLG-3 updated to assert the new node and edge set.
  - Pytest after every step: **6/6 still green** (last run: 22.19s).
  - Compose service count: 5 → **3** long-running + 1 one-shot.

### Completed tasks

| ID | Owner | Task | Notes |
|---|---|---|---|
| T0 | Architect | Tracking-file scaffolding | `SPRINTS.md`, `Progress.md`, `requirements.txt`, `.env.example` |
| T1 | Architect | Contract document | `docs/remote-langgraph-contract.md` (binding) |
| T2 | Dev | Orchestrator service | `services/orchestrator/main.py` (~342 lines), 8 endpoints, Redis-backed, OTel-instrumented |
| T3 | Dev | AmazeGraph SDK | `sdk/amaze/langgraph.py` (~415 lines), `OrchestratorClient`, error classes, OTel span per remote-node hop |
| T4+T5 | Dev | A2A research + writer nodes + `_common.py` | `examples/a2a_nodes/{_common,research_node,writer_node}.py` — lifespan registration, `AMAZE_DEBUG_BAD_PATCH=1` switch for ST-RLG-6 |
| T6 | Dev | Main demo program | `examples/remote_langgraph/main.py` — 4 error classes caught, `run-start`/`run-end` events emitted, OTel parent span |
| T7 | DevOps | Compose stack + Dockerfiles | `docker/compose.remote-langgraph.yml`, 3 Dockerfiles. Healthchecks on every long-running service |
| T8 | QA | System tests | `tests/system/test_remote_langgraph.py` covering ST-RLG-1..6, `tests/conftest.py` with compose fixture, `tests/system/run_compose_demo.sh` one-button demo |

### Parallel-task timing table (CLAUDE.md §12)

| Task(s) | Phase | Start | End | Duration | Tokens (approx) |
|---|---|---|---|---|---|
| T0, T1 | P1 (sequential, by main agent) | 18:39 | 18:41 | ~2 min | n/a (main agent) |
| T2 (orchestrator) | P2 (parallel) | 18:41:26 | 18:42:40 | ~75 sec | ~49 800 |
| T3 (SDK) | P2 (parallel) | 18:41:26 | 18:42:50 | ~85 sec | ~53 200 |
| T7 (compose+Dockerfiles) | overlapped with P2 by main agent | 18:41 | 18:43 | ~2 min | n/a (main agent) |
| T4+T5 (a2a nodes) | P3 (parallel) | 18:44:47 | 18:46:00 | ~73 sec | ~40 600 |
| T6 (main demo) | P3 (parallel) | 18:44:47 | 18:46:06 | ~79 sec | ~38 800 |
| T8 (system tests) | overlapped with P3 by main agent | 18:45 | 18:47 | ~2 min | n/a (main agent) |

### Sprint demo — verified working

Command sequence used:
```bash
cd /home/ubuntu/data/cloude/aMazeGraph
docker compose -p amazegraph-test -f docker/compose.remote-langgraph.yml build
docker compose -p amazegraph-test -f docker/compose.remote-langgraph.yml up -d redis jaeger orchestrator a2a-research a2a-writer
docker compose -p amazegraph-test -f docker/compose.remote-langgraph.yml run --rm main-langgraph
```

Observed output (key lines):
```
main: orchestrator URL=http://orchestrator:8001
main: graph compiled
main: invoking graph
main: graph completed
FINAL RESULT: Final answer for 'Create short architecture for remote LangGraph execution': Research summary for: ...
main: final_answer present
```

`GET http://localhost:8011/runs/run-1` shows ordered events:

| event | node_name | status |
|---|---|---|
| run-start | - | running |
| node-enter | research | - |
| node-exit | research | ok |
| node-enter | writer | - |
| node-exit | writer | ok |
| run-end | - | done |

Final meta status: `done`.

`GET http://localhost:16696/api/traces?service=main-langgraph` returns 2 traces. The full-run trace has **51 spans** — `main-langgraph.run` (parent) → `amazegraph.invoke_remote` (per remote hop) → `POST /invoke` server spans on remote nodes → outbound httpx auto-spans for resolve and event emission. Trace correlation across services confirmed.

### Test runs

**2026-04-30 first pass:** 4/6 passed (ST-RLG-1, 2, 3, 4). 2 failed for fixture reasons (not production bugs):
- ST-RLG-5 — `stop_service("a2a-writer")` was nullified by `docker compose run`'s `depends_on: condition: service_healthy` bringing the writer back up before the demo ran.
- ST-RLG-6 — `--service-ports` collided on host port 9012 because `docker compose run` had already restarted `a2a-research` for ST-RLG-5's depends_on chain.

**2026-04-30 fixture rewrite + 🟡 fixes applied:**
- `temporarily_unregister_node` fixture — calls `DELETE /register/node` directly (mirrors the contract's graceful-shutdown semantics) and re-registers in teardown. Bypasses the depends_on auto-revive.
- `research_with_env` fixture — launches a sidecar debug container (`{project}-a2a-research-debug`) on the same docker network with its own DNS name. The sidecar's lifespan registers via last-write-wins, overwriting research's endpoint. Teardown removes the sidecar and re-POSTs the original registration.

**2026-04-30 second pass:** `6 passed in 21.91s`. All ST-RLG-1..6 green.

### Code review (2026-04-30)

`/code-reviewer` run on the 6 Sprint 1 source files. Findings: 0 🔴 blocking, 5 🟡 should-fix, 7 🟢 nits. All 🟡 items applied:

| 🟡 | File | Fix |
|---|---|---|
| Path-param regex | `services/orchestrator/main.py` | `Path(pattern=...)` on `resolve_node`, `get_graph`, `append_run_event`, `get_run`. New `RUN_ID_PATTERN`, `ENDPOINT_PATTERN`. |
| Endpoint URL validation | `services/orchestrator/main.py` | `RegisterNodeRequest._validate_endpoint` rejects non-`http(s)://` strings. |
| Atomic `run-end` meta promotion | `services/orchestrator/main.py` | `_RUN_END_PROMOTE_LUA` server-side EVAL using cjson — replaces the non-atomic GET-mutate-SET sequence. |
| `RunnableConfig` typing | `sdk/amaze/langgraph.py` | Proxy signature `(state: dict, config: RunnableConfig | None = None)`. UserWarning silenced. |
| `httpx.TransportError` | `sdk/amaze/langgraph.py` | Replaces `(httpx.ConnectError, httpx.ReadError)` — covers timeouts and other transport-layer faults. |
| `response_model` WHY comment | `examples/a2a_nodes/_common.py` | Multi-line WHY explaining the deliberate omission so ST-RLG-6's bad-patch path isn't blocked server-side. Also tidied JSON log line via `json.dumps`. |

7 🟢 nits remain (deferred, none blocking): `_validate_graph_id` duplication across 3 Pydantic models; `_make_remote_proxy` size could be reduced via `_emit_error` helper; demo's `run-end` emission duplicated 4× (ditto helper); `DELETE /register/node` with body could become path-param form; unused `OrchestratorClient.register_graph` (sync `compile()` reimplements it).

### Open issues for next session

1. **DNS-on-restart class of failures — largely sidestepped** by embedding redis+jaeger into orchestrator (those used to be the most common port-collision triggers). If it recurs on a2a nodes, recovery is still: `docker compose rm -f -s <svc>` then `up -d <svc>`. Decide whether to add `restart: on-failure` or accept the quirk.

2. **Run-id reuse across tests** — the demo hardcodes `run_id="run-1"` so multiple invocations append events to the same Redis Stream. Now even more visible because the embedded Redis is in-memory only — events accumulate within one stack lifetime then disappear on restart. Fix: have `run_main_langgraph` pass `RUN_ID` env var to the demo.

3. **`RunnableConfig` UserWarning still emitted** by LangGraph at import — typing the proxy as `RunnableConfig | None` triggers a warning that says we typed it as `RunnableConfig | None`, which is a `from __future__ import annotations` interaction. Cosmetic. Fix is one line: drop `from __future__ import annotations` from `sdk/amaze/langgraph.py`, or import via `typing.Optional[RunnableConfig]`.

4. **`log_trail` last-write-wins** — the new `post_research`/`post_writer` local nodes append to `log_trail` via plain list overwrite. Sprint 2 will introduce parallel branches via #5/#6 in the roadmap; convert `log_trail` to `Annotated[list[str], operator.add]` then so concurrent appends merge.

5. **🟢 nits cleanup pass** (none blocking; do whenever convenient):
   - Extract `_emit_error_event(...)` helper inside `_make_remote_proxy` to dedupe 5 near-identical event-emit blocks.
   - Extract `_emit_run_end(...)` helper in `examples/remote_langgraph/main.py` to dedupe 4 copies.
   - Drop unused `OrchestratorClient.register_graph` or refactor `compile()` to use it.
   - `DELETE /register/node` — switch from body-bearing to path-param form for HTTP-intermediary friendliness.

6. **Re-extract Redis and Jaeger** before any production deployment. Both are bundled into the orchestrator container as a "meanwhile" arrangement. Anti-pattern for k8s-style deployments without proper supervisor/signal-handling. Cleanest re-extraction would put each as its own compose service with proper volumes/healthchecks once we have a real deployment target.

### Original session-1 issues — all resolved

- ✓ Pytest suite green (6/6).
- ✓ a2a-research DNS-failure root-caused (orphaned container with empty `NetworkSettings.Networks`); recovery documented.
- ✓ All 5 🟡 code-review items applied.
- ✓ Redis port collision class of failures eliminated (embedding).

### Stack state at end of session

3 long-running services healthy: `orchestrator` (with embedded redis + jaeger), `a2a-research`, `a2a-writer`. `main-langgraph` is one-shot; runs to completion and exits 0. Demo verified end-to-end multiple times today. All 6 system tests green (last run: 22.19s).

### Port remapping (host side, current)

Coexisting `amaze-platform` stack already binds 6379, 8001, 9002, 9003, 16686, 4317. Our compose publishes only what the developer needs from the host (everything else stays internal-only):

| Service | Internal | Host (current) |
|---|---|---|
| orchestrator HTTP | 8001 | **8011** |
| Jaeger UI (in orchestrator) | 16686 | **16696** |
| a2a-research /healthz | 9002 | **9012** |
| a2a-writer /healthz | 9003 | **9013** |

Internal-only (no host port): redis 6379, Jaeger OTLP 4317, Jaeger health 14269. Redis and Jaeger live inside the orchestrator container.

**Old port remap (preserved for reference)** — these were used while redis and jaeger were separate compose services:

| Service | Internal | Host (deprecated) |
|---|---|---|
| redis | 6379 | ~~6380~~ |
| jaeger UI | 16686 | ~~16696~~ (now via orchestrator) |
| jaeger OTLP gRPC | 4317 | ~~4337~~ (no longer published) |
| jaeger health | 14269 | **14279** | yes |
| orchestrator | 8001 | **8011** | yes |
| a2a-research | 9002 | **9012** | yes |
| a2a-writer | 9003 | **9013** | yes |

All inter-container traffic uses the original internal ports via the docker network (e.g. `http://orchestrator:8001`, `http://jaeger:4317`). Only the host-side bindings differ. The Sprint Demo Script in the original plan used `localhost:8001` etc. — when reading those, mentally substitute the host port from this table.

### Code-review status

`/code-reviewer` complete on all 6 Sprint 1 source files. Findings: 0 🔴 / 5 🟡 / 7 🟢. All 🟡 applied (see Code review section above). 7 🟢 nits enumerated in "Open issues for next session" item 3.

### Next session checklist

1. Sprint 1 retrospective with user (if desired).
2. Sprint 2 planning conversation — reducers, conditional edges, parallel edges, opaque subgraph nodes, plus the test-smell cleanup (#2) and 🟢 nit pass (#3) above. Roadmap reference: `/home/ubuntu/.claude/plans/i-want-to-enhance-tranquil-moler.md` § "Roadmap — implications of upcoming LangGraph capabilities".

### Pytest replay command (for the next session)

If the stack is up and healthy:
```bash
AMAZEGRAPH_SKIP_COMPOSE=1 \
ORCHESTRATOR_URL=http://localhost:8011 \
JAEGER_URL=http://localhost:16696 \
REDIS_PORT=6380 \
RESEARCH_HEALTH_URL=http://localhost:9012/healthz \
WRITER_HEALTH_URL=http://localhost:9013/healthz \
/home/ubuntu/venv/bin/python -m pytest tests/system/ -v --tb=short
```

Or let pytest manage the compose lifecycle: drop `AMAZEGRAPH_SKIP_COMPOSE=1`. The `compose_stack` session fixture in `tests/conftest.py` will run `up --build` then `down -v` around the suite.
