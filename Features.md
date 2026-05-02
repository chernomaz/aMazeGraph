# aMazeGraph — LangGraph node capabilities

The 28 LangGraph node behaviors the distributed runtime must cover, ordered by implementation effort against the **current code** (post-Sprint 1).

Effort definitions:
- **Easy** — already works under the S1 contract; effort is one demo + one system test, no contract or infra change.
- **Medium** — small contract or schema addition (sibling key, JSON helper, validator, decorator option). No new long-running component.
- **Hard** — new infrastructure or transport (checkpointer, store, SSE stream, cache, dynamic dispatch, parent-graph linkage, sibling response keys with resume semantics).

---

## EASY (8) — already supported, just needs a demo + test

| # | Case | Notes |
|---|---|---|
| 1 | Read graph state | Full state already shipped in `/invoke` body. |
| 2 | Return partial state update | `state_patch: dict` is the S1 contract. |
| 5 | Call LLM inside node | Option A: match LangGraph's stock behavior — no interception, no enforcement. Observability via OTel `traceparent` + LangSmith env-var sharing on both sides. |
| 6 | Call tools inside node | Same as #5. `tool.invoke(...)` is plain Python; LangSmith/OTel auto-instrument. |
| 7 | Async node | Proxy is already `async def`; `serve_node` `await`s the handler. |
| 8 | Access config: thread_id / tags / metadata | `config_subset` already propagated; `thread_id` lives under `configurable`. |
| 10 | Static routing through edges | `add_edge` passthrough; 5-node demo already exercises a chain. |
| 26 | Return nothing / no-op update | `state_patch={}` passes the dict check. |

---

## MEDIUM (10) — small contract / schema / validator addition

| # | Case | What changes |
|---|---|---|
| 3 | Reducers (`operator.add`, custom callables) | Schema annotation: `Annotated[list, operator.add]` etc. Reducer runs **locally** in the driver — zero proxy change. Document the rule "any field two branches write needs a reducer." |
| 4 | MessagesState / `add_messages` | JSON helper to (de)serialize `BaseMessage` ↔ dict on both proxy and `serve_node` paths. Reducer is local. |
| 9 | Runtime context (read-only fields like `tenant_id`, `model_name`) | Add `runtime_context: dict` (JSON-serializable subset) to `/invoke`; `serve_node` reconstructs a stub `Runtime` with `.context` populated. `runtime.store` and `.stream_writer` raise — they're cases #22 and #24. |
| 11 | Conditional routing | Stub already exists in SDK; needs working demo where a router targets a remote node, plus a system test. |
| 12 | Parallel fan-out via multiple edges | Reducers (#3) + httpx pool size review + concurrent-XADD assertion (already pure append). Demonstrated by overlapping wall-clock timestamps in `node-enter` events. |
| 19 | Subgraph as node (opaque) | Remote node internally compiles its own `StateGraph` and returns one `state_patch`. Works under S1 contract — register as a single `node_name`. |
| 20 | Call subgraph manually inside node | Variant of #19; same effort. |
| 25 | Recursion / step metadata | Ensure `config["metadata"]["langgraph_step"]` and `recursion_limit` survive the wire (already in subset). Add denial-of-wallet test. |
| 27 | Richer error taxonomy | Extend `node-error` event with `error_kind`: `node_error`, `policy_violation`, `timeout`, `proxy_block`, `tool_error`. |
| 28 | Different input/output/private schemas | Pure LangGraph feature; works at driver level. Demo split `InputState`/`PrivateState`/`OutputState`. |

---

## HARD (10) — new infrastructure / transport

Ordered by **dependency chain**, not raw difficulty (later rows often depend on earlier):

| # | Case | New thing required | Gates |
|---|---|---|---|
| 14 | `Command(update, goto)` | Response **sibling key** `command: {update, goto}`. Orchestrator validates `goto` against the registered graph manifest at proxy time. | None — lands first in this tier. |
| 15 | `Command(goto=[multi])` | Extension of #14 — list-typed `goto`. | After #14. |
| 13 | `Send(node, payload)` | Wire-format change: `/invoke` body grows a `dispatch: {kind: "send", payload}` mode; orchestrator schedules N parallel invokes with per-call payloads. | Reducers (#3) live; pool sizing from #12. |
| 16 | `Command + Send` | Combine #14 sibling + #13 wire variant. | After #13 + #14. |
| 23 | Persistent state / checkpointing | New component: wire `RedisSaver` (recommended) into the driver compile path. Orchestrator's run-event stream is **not** a substitute. | Pure infra; **gates #17 #18 #21**. |
| 17 | `interrupt()` from inside node | Response sibling `interrupt: {value, id}`; driver stores in checkpointer; resume via second `/invoke` carrying `Command(resume=...)`. | Requires #23. |
| 18 | Human validation loop | #17 + idempotency contract (resume re-executes node, so pre-`interrupt` side effects must be idempotent). | Requires #17. |
| 21 | `Command(graph=Command.PARENT)` | Manifest gains parent-graph linkage; proxy translates parent-graph routing into orchestrator-side handoff. | Requires #14 + #23. |
| 22 | Node-level caching | New orchestrator surface (`GET/PUT /cache/{key}`); cache key spec; TTL/eviction. | Standalone. |
| 24 | Stream custom output from node | `/invoke` negotiates `Accept: text/event-stream`; chunks each carry `state_patch | command | events`. Run-event Redis stream stays the durable record. | Standalone, but heavy. |

---

## Sprint sequencing

| Sprint | Cases | Theme |
|---|---|---|
| **S2** *(active)* | EASY-block (1, 2, 5, 6, 7, 8, 10, 26) + MED-A (3, 4, 11, 12) | "Reducers + conditional + parallel + messages + LLM/tool demos" |
| S3 | MED-B (9, 19, 20, 25, 27, 28) | "Opaque subgraphs + runtime context + error taxonomy" |
| S4 | HARD-A (14, 15) | `Command` response sibling |
| S5 | HARD-B (13, 16) | `Send` wire format |
| S6 | HARD-C (23, 17, 18) | Checkpointer + interrupts |
| S7 | HARD-D (21, 22, 24) | Parent-graph + cache + streaming |

Each sprint introduces at most one new sibling key or one new orchestrator surface (CLAUDE.md §13: never change architecture on own judgement).

---

## Open architectural questions

1. **Cases 5/6 (LLM/tool calls): RESOLVED — Option A.** Match LangGraph's stock behavior: no interception, observability via OTel + LangSmith env-var sharing.
2. Case 23 checkpointer: `RedisSaver` recommended (reuses existing Redis). Confirm before S6.
3. Case 22 cache placement: orchestrator-side recommended (cross-driver hits). Confirm before S7.
