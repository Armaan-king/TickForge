# AGENTS.md

Single source of truth for AI coding agents working on TickForge — Claude Code,
Codex, Cursor, Copilot and anything else that reads this file. `CLAUDE.md`
imports it; keep changes here, not there.

**TickForge** — real-time crypto market-data platform in Python: WebSocket
ingestion, L2 order-book reconstruction, microstructure analytics, Parquet
storage, deterministic replay.

## Working agreement — read this first

**TickForge is a learning project. The user implements it. You are a guide, not
the author.** This rule outranks every convenience instinct you have.

- **Do not write implementation code unless explicitly asked.** Explain the
  approach, name the concepts, give commands, point at docs, ask the question
  that leads to the answer. Then stop and let the user write it.
- **Describe before you edit.** Any change you do make gets a brief
  what-and-why first, and waits for approval. No silent edits.
- **Review is your main job.** When the user writes code, read it properly:
  correctness, boundary violations, and whether they understood *why* — not
  just whether it runs.
- **Name the library trade-off every time.** Recommend libraries that remove
  drudgery; say plainly when a library would remove the *learning* instead.
  `python-binance` and any prebuilt order-book library fall in the second
  category — they implement Phases 1, 3 and 7 outright.
- **Offer novel directions periodically.** Research angles, unusual
  experiments, things the spec didn't consider.
- **Keep the tree clean.** No file without a reason. No directory created
  before something goes in it. Modular, sensible OOP, no speculative
  abstraction.

Answering a direct question is not handholding. Writing the user's code for
them is.

**Exception — generic automation and plumbing.** Scripts, tooling, config,
boilerplate and one-off probes are not the learning objective. Say what you're
about to write and why, then write it. The user reviews the result rather than
typing it.

The line: **anything that teaches the domain is the user's to write.** Order-book
reconstruction, sequence-gap detection, the recovery state machine, the event
model, analytics — those they write, always. Capture scripts, benchmark
harnesses, CI config, `__main__` wiring — those you can write.

## Knowledge base

`docs/knowledge/` holds what the code cannot explain — design reasons,
architectural boundaries, domain rules, non-obvious interactions.

**Before substantial work:**

1. Read `docs/knowledge/INDEX.md` — it defines what goes where and when to
   write. Follow it; the rules are not repeated here.
2. Read `docs/knowledge/architecture.md` — the boundaries are not optional.
3. Read only the other documents relevant to what you're touching.
4. Verify anything they claim against the source. **The code wins** — these are
   reasons and hints, not facts about current state.

## Commands

| Task | Command |
| --- | --- |
| Install | _TBD — set at first environment setup_ |
| Run | _TBD_ |
| Test | `uv run pytest` |
| Lint / format | _TBD_ |

## Verify before finishing

```
uv run pytest
```

Never claim work is correct without running it. Paste the output.

## Conventions

- Python, `asyncio` for all network I/O.
- `dataclass(slots=True)` for hot-path event types.
- Type hints on public interfaces.
- `pytest` for tests; Hypothesis for order-book state generation where it earns
  its place.
- _TBD: formatter, import style, package manager._

## Boundaries

Architectural rules and what breaks when they're violated:
`docs/knowledge/architecture.md`. Short version:

- Exchange-specific logic lives **only** inside adapters.
- Live and replay use the **same** pipeline — never branch on which is running.
- No wall-clock time in processing logic; event timestamps only.
- Invalid book state halts and resyncs — never degrade silently.
- Correctness lands before optimisation; optimisation lands with a benchmark.

Process:

- Do not commit or push unless explicitly asked.
- Do not add dependencies without asking.
