# CLAUDE.md

Guidance for Claude Code in this repository.

Commands, conventions, boundaries and verification rules live in @AGENTS.md —
that file is the single source of truth for all coding agents. Read it first,
and edit it rather than this file when those change.

## What TickForge is

Python platform for real-time crypto market data: WebSocket ingestion, L2
order-book reconstruction, microstructure analytics, Parquet storage, and
deterministic replay through the same pipeline as live data.

Market-data **infrastructure and research tooling** — not a matching engine,
not a low-latency trading system.

## Context

Auto-loaded: @docs/knowledge/INDEX.md and @docs/knowledge/architecture.md

Read on demand, not auto-loaded:

- `docs/Goal.md` — the full 10-phase spec. Long; read the phase you're working
  on, not the whole file.
- `docs/knowledge/decisions.md` — before proposing an architectural change,
  check whether it was already decided and rejected.
- `docs/knowledge/pitfalls.md` — before writing ingestion, book or replay logic.
- `docs/knowledge/concepts/` — one file per component, once it exists.

## Claude-Code-specific

- Ask before adding a dependency.
- Prefer the smallest change that solves the problem.
