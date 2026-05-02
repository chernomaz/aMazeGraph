## 1. Project goal

**aMazeGraph builds full support for remote-node execution in LangGraph.**

A LangGraph program runs in one process (the driver), but specific nodes
execute as standalone services on different hosts — registered in a Redis-backed
orchestrator and invoked over HTTP. The driver stays LangGraph-native
(`StateGraph`, `add_edge`, `app.ainvoke`); the only new author surface is
`@remote_node` and `serve_node()`.

The 28 LangGraph node capabilities catalogued in `Features.md` are the binding
scope: every behavior that works in a single-process LangGraph node must work
end-to-end when that node runs remotely. See `Features.md` for the
effort-ordered roadmap and `SPRINTS.md` for the active sprint plan.

Non-goals (explicitly out of scope):
- Replacing or forking LangGraph itself. The driver always uses upstream
  LangGraph; we extend, never substitute.
- LLM/tool **enforcement** (token budgets, allowlists, PII filtering). We
  match LangGraph's stock observability story; enforcement, if needed later,
  is a separate platform feature.
- Multi-tenant access control, billing, UI dashboards. Out of scope unless
  added explicitly to a sprint plan.

---

## 10. Environment

- **Python venv:** `/home/ubuntu/venv/`. Always use `/home/ubuntu/venv/bin/python`
  and `/home/ubuntu/venv/bin/pip`. Never use system `python3`/`pip3`.

---

## 11. Bash command permissions

Commands that operate inside `/home/ubuntu/data/cloude/aMazeGraph`
may be run without asking. Ask first only when the command:

- operates outside this directory,
- uses `sudo`,
- changes file permissions (`chmod`), ownership (`chown`), or group (`chgrp`),
- deletes or modifies files outside this directory.

---

## 12. Scrum Process

**Every iteration must deliver a working demo.** No "almost done" — each
sprint ends with something runnable.

### Sprint Rules

1. **Demo first** — each iteration ends with a browser/app demo the user can
   interact with. If it can't be shown, it isn't done.
2. **System tests per iteration** — every sprint ships at least one
   system-level test. Tests are discussed and agreed with the user *before*
   implementation starts.
3. **Test list is collaborative** — before writing any test, present the
   proposed test scenarios to the user and get sign-off. Never write tests
   unilaterally.
4. **No skipping iterations** — if a feature isn't ready, ship a smaller
   slice that is working rather than delivering nothing.
5. **Code review at sprint end** — run `/code-reviewer` on all new/modified
   files at the end of every sprint. Present the full results to the user
   and wait for instructions on what to fix.

### Sprint Task Format

Every sprint is broken into tasks with assigned roles:

| Role | Responsibility |
|---|---|
| Arch | Design decisions, schemas, data models |
| Dev | Implementation |
| QA | System tests |
| GUI | UI implementation (Sprint 3+) |

Tasks include explicit dependencies and are grouped into parallel phases.

### Parallel Execution

Independent tasks (no shared files, no data dependencies) **must** be run in
parallel using background agents. The rules:

1. **Identify phases** — group tasks into phases where every task within a
   phase has no file overlap with any sibling in the same phase.
2. **Launch simultaneously** — send all agents in a single message so they
   execute concurrently. Never launch them one at a time.
3. **No shared files per phase** — if two tasks in a proposed phase would
   touch the same file, split them into sequential phases.
4. **Timing log** — record each agent's wall-clock start time, end time, and
   token count as it completes. Write results to `sprint_timing.md` (or
   append to Progress.md) at the end of the sprint so cost and parallelism
   gains are visible.

Timing table format (append to Progress.md at sprint end):

```
| Task | Start | End | Duration | Tokens |
|------|-------|-----|----------|--------|
| T3-1 | 14:02 | 14:07 | 5 min | ~4 200 |
```

### System Tests (not unit tests)

- Tests must exercise the full stack end-to-end (agent → proxy → addon →
  decision).
- No mocking of the enforcement path.
- Never use mocks in system tests.
- Each test must be runnable as a standalone command.
- Every test verifies: (1) correct HTTP response, (2) correct audit log
  sequence in Redis Stream, (3) matching OTel trace in Jaeger.
- Use `agent_sdk.py`, `agent_sdk1.py`, `agent_sdk2.py` as drivers — do not
  modify them.

### Tracking Files

- **`SPRINTS.md`** — full sprint plan with all agreed system tests per sprint;
  updated at sprint start. Task rows include a status column (☐ / ◐ / ✓).
- **`Progress.md`** — running log of what has been completed; updated as work
  is finished. Includes timing table for parallel tasks at sprint end.

---

## 13. Key Rules for Claude

- **Never change architecture on your own judgement.** Raise the question,
  explain the trade-off, and wait for explicit approval before making any
  architectural change.
- Do NOT introduce new abstractions beyond what the task requires.
- Prefer simple, enforceable rules over clever logic.
- Never commit secrets. `.env` and any `*.secret.*` files stay out of git.

---