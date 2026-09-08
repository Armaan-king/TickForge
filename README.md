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

Early. Normalized event model in place; ingestion next.

## Quick start

```
uv sync
uv run pytest
```

Start the HTTP API (tracks BTCUSDT using the live Binance feed):

```text
uv run uvicorn tickforge.api:app
```

Open http://127.0.0.1:8000/docs for interactive endpoint documentation.
The API exposes `/markets/BTCUSDT/book`, `/markets/BTCUSDT/features`,
`/markets/BTCUSDT/trades`, and `/system/health`. Prices, quantities, and
decimal features are JSON strings to preserve exact values.

Health returns HTTP 200 with a `status` of `ok` or `degraded`; consumers
must inspect the body. Before the first snapshot, book requests return 503.
Features return 404 until a valid update arrives, including after a resync.

## Docs

- [Goal](docs/Goal.md) — the full 10-phase spec and success criteria.
- [Knowledge base](docs/knowledge/INDEX.md) — architecture boundaries, decisions, pitfalls.
- [AGENTS.md](AGENTS.md) — commands and rules for AI coding agents.

## License

_TBD_
