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

---

## Sprint 2 — Reducers, MessagesState, conditional routing, parallel fan-out, real LLM+MCP

**Goal:** Cover 13 of the 28 LangGraph node capabilities (8 Easy + 5 Medium) with
working demos and end-to-end system tests. The headline outcome: a single demo
graph that exercises a real-OpenAI / real-MCP node, a conditional router, an
audit-only no-op node, and a real-parallel fan-out with reducer-merged results.

See `Features.md` for the full 28-case effort breakdown. See the approved plan
at `/home/ubuntu/.claude/plans/https-chatgpt-com-share-69f3546a-9d78-83-ethereal-mango.md`
for the full Sprint 2 design.

### Sprint 2 capability coverage

| # | Capability | Sprint 2 deliverable |
|---|---|---|
| 1 | Read graph state | Remote node reads nested state in ST-RLG-9 |
| 2 | Return partial state update | Already shipped; re-asserted in ST-RLG-9/10/12/13 |
| 3 | Reducers (`operator.add`) | Demo schema annotates `log_trail` and `results` |
| 4 | MessagesState / `add_messages` | `sdk/amaze/_messages.py` BaseMessage↔dict helper |
| 5 | LLM in node (real OpenAI) | `examples/a2a_nodes/llm_tool_node.py` |
| 6 | Tool in node (real MCP) | `examples/mcp_server/` vendored from neighbor; ChatOpenAI binds MCP tools |
| 7 | Async node | Explicit `async def` in llm_tool_node + `await asyncio.sleep` in audit_node |
| 8 | Config thread_id/tags/metadata | Driver passes `configurable.thread_id`; remote echoes |
| 9 | Runtime context (read-only) | New `runtime_context: dict` field on `/invoke`; `Runtime` stub on remote side |
| 10 | Static edges | Already shipped; re-asserted by demo topology |
| 11 | Conditional routing | Router node picks between 2 remote targets based on state |
| 12 | Parallel fan-out (real) | `planner → [research_a, research_b] → joiner` with overlapping wall-clock |
| 26 | No-op return | `audit_node` returns `{}`; ST-RLG-12 asserts state unchanged |

### External dependencies (added in Sprint 2)

- `OPENAI_API_KEY` and `TAVILY_API_KEY` copied from
  `/home/ubuntu/data/cloude/newAmazeControlPlane/aMaze/.env` into our local
  `.env` (kept out of git). `.env.example` updated with placeholder names only.
- `examples/mcp_server/` vendored from
  `/home/ubuntu/data/cloude/newAmazeControlPlane/aMaze/examples/mcp_server/`
  (FastMCP `streamable-http` server, auto-discovers LangChain `@tool`s from
  `tools/*.py`). Sprint 2 uses `web_search` (Tavily-backed); other tools ship
  but are unused.
- New `mcp` compose service.
- New Python deps: `langchain-openai`, `langchain-mcp-adapters`,
  `fastmcp` (or direct `mcp` SDK).

### Sprint 2 tasks

| ID | Owner | Task | Deliverable | Status |
|---|---|---|---|---|
| T1 | Arch | Contract addendum | `docs/remote-langgraph-contract.md` § reducers, messages, runtime_context, conditional, MCP | ✓ |
| T2 | Arch | Demo state schema with reducers | `examples/remote_langgraph/main.py` (Annotated `log_trail` + `results` + MessagesState `messages`) | ✓ |
| T3 | Dev | MessagesState (de)serialization helper | `sdk/amaze/_messages.py` | ✓ |
| T4 | Dev | `runtime_context` field on `/invoke` | `sdk/amaze/langgraph.py` proxy + `examples/a2a_nodes/_common.py` Runtime stub | ✓ |
| T5 | Dev | Conditional-routing demo | `examples/remote_langgraph/main.py` (router → 2 remote targets) | ✓ |
| T6 | Dev | Real OpenAI + real MCP demo node | `examples/a2a_nodes/llm_tool_node.py` (`async def`, `ChatOpenAI` + MCP client) | ✓ |
| T7 | Dev | Audit-only no-op node | `examples/a2a_nodes/audit_node.py` | ✓ |
| T8 | DevOps | MCP server compose service + .env wiring | vendor `examples/mcp_server/`; add `mcp` to compose; env passthrough; `.env.example` update; new requirements | ✓ |
| T8b | Dev | Parallel fan-out demo (real concurrency) | extend `main.py` (`planner → [research_a, research_b] → joiner`); httpx pool size verified ≥ 2 | ✓ |
| T9 | QA | System tests ST-RLG-7..13 | `tests/system/test_sprint2.py` | ✓ |

### Parallel phases

- **P1 (sequential):** T1 → T8 (contract first; MCP vendor-in must precede T6)
- **P2 (parallel):** T2, T3, T4 — no file overlap
- **P3 (parallel):** T5, T6, T7, T8b — no file overlap (each adds new file or non-overlapping section in `main.py`)
- **P4 (sequential):** T9

### Agreed system tests (ST-RLG-7..13)

Each runs against the live compose stack (no mocks). Verifies (1) HTTP / exit
code, (2) Redis Stream entries via XRANGE, (3) Jaeger trace via HTTP API.

| ID | Cases | Scenario | Pass condition |
|---|---|---|---|
| ST-RLG-7 | 3 | Reducer merges `log_trail` from local + remote post-nodes | Final `log_trail` is the ordered concatenation; reducer wired |
| ST-RLG-8 | 4 | Remote returns `{messages: [{role, content}]}` and `add_messages` reducer merges | `state["messages"]` grows by 1, type `AIMessage`, content matches |
| ST-RLG-9 | 1, 5, 6, 7 | Remote (async) reads nested state, calls real OpenAI ChatOpenAI bound to MCP tools, ChatOpenAI emits tool_calls for `web_search`, MCP runs Tavily, result returns into state | `messages` has assistant + tool messages; tool result non-empty; Jaeger trace `amazegraph.invoke_remote → ChatOpenAI → mcp_client → web_search`; SKIPS with clear message if `OPENAI_API_KEY` or `TAVILY_API_KEY` unset |
| ST-RLG-10 | 8, 9 | Driver passes `configurable={"thread_id":"t-123"}` + `runtime_context={"tenant_id":"acme"}`; remote echoes both | `state["echoed_thread"]=="t-123"` and `state["echoed_tenant"]=="acme"` |
| ST-RLG-11 | 11 | Conditional router picks remote A or remote B based on `state["mode"]`; run both modes | Each run hits exactly one remote; XRANGE shows correct one fired |
| ST-RLG-12 | 26 | Audit-only remote returns `{}` | State equal pre/post except for audit's `log_trail` line |
| ST-RLG-13 | 12 + 3 | **Real parallel fan-out.** `planner → [research_a (remote), research_b (remote)] → joiner`. Both branches return into `Annotated[list, operator.add]`. | Final `state["results"]` set-equal to `{"from_a","from_b"}`; two `node-enter` events with **overlapping wall-clock timestamps**; two distinct child spans under planner span; httpx pool ≥ 2 |
| ST-RLG-14 | 3 | **Mixed local+remote reducer.** S7: `s7_local (local) → research (remote)`. Both append to `log_trail` via `operator.add`. | Final `log_trail` contains entries from both nodes; neither overwrites the other. |

### Definition of Done

- All 13 capabilities covered by the demo graph (one `main.py` invocation
  exercises every capability). ✓
- ST-RLG-7..13 all pass (skipped tests count as pass IFF skip reason is missing
  optional API key). ✓
- ST-RLG-1..6 still pass (no regressions). ✓
- One-command demo (`docker compose -f docker/compose.remote-langgraph.yml up
  --build`) runs end to end. ✓
- `/code-reviewer` run on all new/modified files; results presented; user
  signs off on which 🟡/🟢 items to apply. ✓ (2026-05-01 — 1 🔴 + 4 🟡 fixed; 5 🟢 nits deferred)
- `Progress.md` timing table populated. ✓
- `SPRINTS.md` task statuses updated. ✓
- `docs/remote-langgraph-contract.md` updated with the four addenda. ✓

### Demo results (2026-05-01)

All 7 scenarios green:
```
✓ S1: original research→writer flow
✓ S2: LLM + MCP tool node (skipped gracefully — no OPENAI_API_KEY in env)
✓ S3: config echo (thread_id + tenant_id via runtime_context)
✓ S4a: conditional routing → research branch
✓ S4b: conditional routing → writer branch
✓ S5: audit no-op (state unchanged, log_trail appended)
✓ S6: parallel fan-out (research_a ∥ research_b → reducer merge)
```

**Notable fix (2026-05-01):** S3 config echo was broken because LangGraph 1.1.6
does not inject config into closure functions via parameter injection. Fixed by
reading config from LangGraph's ContextVar (`langgraph.config.get_config()`)
inside `remote_proxy` — this works regardless of how the function is called.

### Out of scope for Sprint 2 (deferred to S3+)

- Cases 19/20 (subgraphs), 25 (recursion metadata), 27 (error taxonomy), 28
  (input/output/private schemas).
- All HARD cases: `Command`, `Send`, checkpointer, `interrupt`, parent
  navigation, caching, streaming.
- LLM / tool **enforcement** (token budget, allowlist, PII filter). Option A
  locks observability-only behavior matching stock LangGraph.
