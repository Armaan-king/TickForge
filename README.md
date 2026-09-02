# TickForge

Real-time crypto market-data platform in Python. Ingests exchange WebSocket
feeds, reconstructs Level-2 order books, computes market microstructure
features, persists events to Parquet, and replays them deterministically
through the same pipeline used for live data.

Market-data **infrastructure and research tooling** — not a matching engine,
not a low-latency trading system.

```
Exchange WebSocket → Adapter → Normalized Events → Async Pipeline
    → L2 Book Reconstruction → Microstructure Analytics
    → Storage + Replay → Research/API
```

## Status

Pre-implementation. Spec and knowledge base in place; no code yet.

## Quick start

```
TBD
```

## Docs

- [Goal](docs/Goal.md) — the full 10-phase spec and success criteria.
- [Knowledge base](docs/knowledge/INDEX.md) — architecture boundaries, decisions, pitfalls.
- [AGENTS.md](AGENTS.md) — commands and rules for AI coding agents.

## License

_TBD_
