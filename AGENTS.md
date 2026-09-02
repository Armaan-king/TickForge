# AGENTS.md

Single source of truth for AI coding agents working on TickForge — Claude Code,
Codex, Cursor, Copilot and anything else that reads this file. `CLAUDE.md`
imports it; keep changes here, not there.

TickForge — _TBD: one-line description._

## Knowledge base

`docs/knowledge/` holds what the code cannot explain — design reasons,
architectural boundaries, domain rules, non-obvious interactions.

**Before substantial work:**

1. Read `docs/knowledge/INDEX.md` (the index).
2. Read only the documents relevant to what you're touching.
3. Verify anything it claims against the source. **The code wins** — these are
   reasons and hints, not facts about current state.

**After:** write a note if you were *surprised*, had to be told a "why" that
wasn't in the code, made a decision a future reader could reasonably undo, or
hit a trap worth warning about. Not for renames, refactors or new functions.

Never record what `rg` can answer — imports, call sites, signatures. A stale
relationship read as truth is worse than no document at all.

## Commands

| Task | Command |
| --- | --- |
| Install | _TBD_ |
| Run | _TBD_ |
| Test | _TBD_ |
| Lint / format | _TBD_ |

## Verify before finishing

Run these and confirm they pass before claiming work is done:

```
TBD
```

## Conventions

- _TBD: language, formatter, import style._
- _TBD: test layout and naming._
- _TBD: commit message style._

## Boundaries

- _TBD: files or directories never to edit (generated, vendored, secrets)._
- _TBD: anything needing confirmation before running (migrations, deploys)._
- _TBD: scope limits — what an agent may change unattended._
- Do not commit or push unless explicitly asked.
- Do not add dependencies without asking.

## Agent roster

_TBD: if TickForge defines its own agents/subagents, list them here._

| Agent | Purpose | When to use |
| --- | --- | --- |
| _TBD_ | _TBD_ | _TBD_ |
