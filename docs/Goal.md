# Goal — Real-Time Market Data & Microstructure Platform

## Objective

Build a production-style Python market-data platform that consumes live exchange data, reconstructs Level-2 order books, computes market microstructure features, stores historical events, and supports deterministic replay for quantitative research and machine learning workflows.

The project is intended to demonstrate practical Python quantitative-development skills across asynchronous programming, market-data engineering, system design, reliability, testing, storage, profiling, and performance optimisation.

The order book is an important component of the system, but the project is primarily focused on **market-data infrastructure and research tooling**, rather than building an ultra-low-latency exchange matching engine.

---

## Core Architecture

The system will follow an event-driven pipeline:

```text
Exchange WebSocket
        ↓
Exchange Adapter
        ↓
Normalized Market Events
        ↓
Async Event Pipeline
        ↓
L2 Order Book Reconstruction
        ↓
Microstructure Analytics
        ↓
Storage + Replay
        ↓
Research / API Interface
```

Exchange-specific formats will be isolated behind adapters.

```text
Binance ──→ BinanceAdapter ──┐
                             │
OKX ──────→ OKXAdapter ──────┼──→ Normalized Market Events
                             │
Future Exchanges ────────────┘
```

The rest of the system should therefore remain independent of any individual exchange.

---

## Phase 1 — Real-Time Market Data Ingestion

The first stage will establish reliable real-time connections to cryptocurrency exchanges using WebSockets.

The initial implementation will support one exchange before expanding to additional venues.

The ingestion layer will consume:

* Level-2 order book updates
* Order book snapshots
* Trade events
* Exchange timestamps
* Sequence or update identifiers

Python's `asyncio` will be used for concurrent network I/O.

The system should also handle:

* WebSocket disconnects
* Automatic reconnection
* Connection timeouts
* Duplicate messages
* Missing updates
* Out-of-order messages
* Stale connections
* API rate limits
* Malformed exchange messages

The goal is not merely to receive data, but to determine whether the local market state is **complete, current, and trustworthy**.

---

## Phase 2 — Normalized Market Event Model

Exchange-specific messages will be transformed into a common internal representation.

Possible event types include:

```text
BookSnapshot
BookUpdate
Trade
MarketStatus
```

For example:

```python
from dataclasses import dataclass


@dataclass(slots=True)
class BookUpdate:
    exchange: str
    symbol: str
    timestamp_ns: int
    sequence: int
    bids: list
    asks: list
```

The normalized event model should allow downstream components to process data without needing to know which exchange produced it.

This separation will make it easier to:

* add new exchanges
* test individual components
* replay historical data
* compare multiple venues
* reuse the platform for future research

---

## Phase 3 — Level-2 Order Book Reconstruction

The system will maintain an in-memory Level-2 order book representing aggregated liquidity at each price level.

Example:

```text
          ASKS

100.03        4.1
100.02        7.3
100.01        2.6

---------------------

99.99         5.2
99.98         8.7
99.97         3.4

          BIDS
```

The order-book component will:

* load an initial exchange snapshot
* process incremental updates
* insert new price levels
* update existing quantities
* remove empty levels
* maintain best bid and best ask
* maintain configurable market depth
* detect invalid or crossed books
* validate update sequence numbers

Correctness will be prioritised before performance optimisation.

For example, if updates arrive as:

```text
1001
1002
1004
```

the system should detect that update `1003` is missing.

The book should then be considered invalid until the system can safely recover through the exchange's required resynchronisation procedure.

---

## Phase 4 — Market Microstructure Analytics

Once the market state is reconstructed correctly, the platform will derive real-time quantitative features.

Initial analytics will include:

* Best bid
* Best ask
* Bid-ask spread
* Midprice
* Microprice
* L1 order-book imbalance
* L5 order-book imbalance
* L10 order-book imbalance
* Market depth
* Order-flow imbalance
* Trade imbalance
* VWAP
* Realised volatility
* Basic liquidity measures

Example output:

```text
BTC-USDT

Best Bid:       95,204.10
Best Ask:       95,204.20
Spread:         0.10
Midprice:       95,204.15
L5 Imbalance:   0.63
```

The objective is to convert raw exchange messages into clean quantitative information that could later be consumed by:

* research notebooks
* trading strategies
* monitoring systems
* machine-learning models
* market simulators

---

## Phase 5 — Historical Market Data Storage

Raw and processed market events will be persisted for later analysis.

The initial storage layer will use:

* Parquet
* PyArrow
* Polars

A possible dataset layout is:

```text
data/
│
├── binance/
│   └── BTCUSDT/
│       └── 2026-09-02/
│           ├── trades.parquet
│           ├── book_updates.parquet
│           └── features.parquet
```

The project will explore:

* data partitioning
* compression
* storage size
* write throughput
* query performance
* schema design
* timestamp precision

The stored dataset should preserve enough information to accurately reconstruct historical market behaviour.

---

## Phase 6 — Deterministic Historical Replay

Recorded market events will be replayable through the same processing pipeline used for live data.

For example:

```bash
python replay.py \
    --exchange binance \
    --symbol BTCUSDT \
    --date 2026-09-02 \
    --speed 100
```

A core architectural principle will be:

```text
LIVE EXCHANGE DATA ─────┐
                        │
                        ▼
                  MarketEvent
                        ▲
                        │
HISTORICAL REPLAY ──────┘
```

Both live and historical events should therefore use the same interfaces.

Replay should support:

* original event timing
* accelerated playback
* deterministic execution
* debugging specific periods
* offline feature generation
* research experiments

This component will also become useful for future machine-learning projects.

---

## Phase 7 — Reliability and Recovery

Real-world market feeds are imperfect.

The system should explicitly handle failure scenarios rather than assuming ideal data.

Recovery mechanisms will include:

* WebSocket reconnection
* sequence-gap detection
* stale-book detection
* snapshot resynchronisation
* duplicate-event handling
* invalid-message rejection
* structured error logging
* health monitoring

The system should always prefer:

```text
Known invalid state
```

over:

```text
Unknown but silently incorrect state
```

A corrupted order book should never continue operating as if it were valid.

---

## Phase 8 — Testing

The project will maintain a comprehensive automated testing suite using `pytest`.

Tests will include:

### Order Book Tests

* snapshot loading
* price-level insertion
* quantity updates
* price-level removal
* best-bid calculation
* best-ask calculation
* crossed-book detection

### Market Data Tests

* duplicate events
* sequence gaps
* out-of-order updates
* malformed messages
* stale updates

### Recovery Tests

* WebSocket disconnection
* snapshot resynchronisation
* stream restart
* invalid-book recovery

### Replay Tests

* deterministic reconstruction
* correct event ordering
* identical output across repeated runs

Property-based testing using Hypothesis may also be introduced to generate unusual order-book states automatically.

Correctness should remain measurable throughout development.

---

## Phase 9 — Performance Engineering

Performance optimisation will begin only after the system is functionally correct.

The platform will measure:

* events processed per second
* p50 event-processing latency
* p95 latency
* p99 latency
* CPU utilisation
* memory consumption
* storage throughput

Profiling tools may include:

```text
cProfile
py-spy
tracemalloc
pytest-benchmark
```

Different Python implementations and data structures may be compared, including:

```text
dict
list
bisect
sorted containers
dataclass(slots=True)
NumPy
Polars
Numba
```

Optimisation decisions should be based on profiling results rather than assumptions.

The purpose is not to compete with specialised C++ low-latency exchange infrastructure.

Instead, the objective is to understand how far a well-engineered Python system can be pushed for **market-data processing, analytics, and research infrastructure**.

---

## Phase 10 — Research/API Interface

A lightweight FastAPI service may expose live market state and derived analytics.

Example endpoints:

```text
GET /markets/BTCUSDT/book

GET /markets/BTCUSDT/features

GET /markets/BTCUSDT/trades

GET /system/health
```

This will allow external applications to consume the platform without interacting directly with exchange-specific WebSocket feeds.

Possible consumers include:

* research notebooks
* dashboards
* monitoring tools
* trading systems
* machine-learning inference services

---

## Engineering Principles

The project will follow several core principles.

### Correctness Before Performance

A fast incorrect order book is useless.

Market-data correctness and recovery will be implemented before optimisation.

### Exchange Independence

Exchange-specific logic should remain inside adapters.

Core analytics and infrastructure should operate on normalized events.

### Live and Replay Consistency

Live feeds and historical replay should pass through the same internal pipeline wherever possible.

### Measured Performance

Performance claims should be supported by reproducible benchmarks.

### Production-Style Reliability

Failures, corrupted data, reconnects, and missing messages should be treated as normal engineering cases rather than edge cases.

### Research Reusability

The platform should produce clean datasets and interfaces that can support future quantitative and ML research.

---

## Final Deliverables

The completed project should contain:

```text
✓ Real-time exchange WebSocket ingestion

✓ Async Python market-data pipeline

✓ Exchange adapter architecture

✓ Normalized event model

✓ Level-2 order-book reconstruction

✓ Sequence validation

✓ Automatic recovery and resynchronisation

✓ Market microstructure analytics

✓ Historical Parquet storage

✓ Deterministic market replay

✓ Automated testing

✓ Property-based testing where useful

✓ Profiling and performance benchmarks

✓ Research/API interface

✓ Clear documentation and architecture diagrams
```

---

## Future Extension

The platform should eventually provide the data infrastructure required for a second research-focused project:

### Generative Market Simulator

Historical order-flow events and reconstructed Level-2 market states produced by this platform can become the training dataset for a generative machine-learning model.

The future system could learn:

```text
Historical Market State
        +
Previous Order Flow
        ↓
Generative Sequence Model
        ↓
Synthetic Future Orders
        ↓
Market Simulator
        ↓
Synthetic Market Trajectory
```

This means Project 1 acts as the **market-data and infrastructure foundation**, while Project 2 explores modern generative modelling of financial markets.

---

## Success Criteria

The project will be considered successful when it can:

1. Connect to a live exchange and reliably ingest market data.
2. Maintain a correct Level-2 order book over extended periods.
3. Detect and recover from corrupted or incomplete market-data streams.
4. Compute real-time market microstructure features.
5. Persist large volumes of market events efficiently.
6. Replay recorded sessions deterministically.
7. Demonstrate correctness through automated tests.
8. Demonstrate performance through reproducible benchmarks.
9. Expose a clean interface for future quantitative research.
10. Serve as the data foundation for the future generative market-modelling project.
