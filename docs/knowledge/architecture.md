# Architecture

The shape of the system, and where its boundaries are. Full detail lives in
[`../Goal.md`](../Goal.md) — this file holds the parts an agent must not
violate.

## Shape

An event-driven pipeline. Exchange bytes enter on the left, normalized events
flow through a single async pipeline, and everything downstream operates on
those events without knowing which venue produced them.

```
Exchange WebSocket → Adapter → Normalized Events → Async Pipeline
    → L2 Book Reconstruction → Microstructure Analytics
    → Storage + Replay → Research/API
```

TickForge is **market-data infrastructure**, not a matching engine. It
reconstructs and analyses a view of someone else's book; it never matches
orders.

## Boundaries

Each rule states what breaks when it's violated. A rule without a consequence
gets ignored.

### Exchange logic stays inside adapters

- **Rule:** nothing downstream of the adapter layer may branch on exchange
  name, parse an exchange payload, or depend on an exchange's field naming,
  tick size convention, or sequencing scheme.
- **Why:** the whole platform's value is being venue-agnostic. Adding OKX
  should touch one new adapter and nothing else.
- **Breaks if violated:** exchange quirks metastasise into the book, the
  analytics and the storage schema. Adding a venue becomes a refactor of the
  entire system, and per-venue bugs become unreproducible in tests.

### Live and replay traverse the same pipeline

- **Rule:** replay injects `MarketEvent`s at the same seam live ingestion does.
  No component may detect or behave differently based on whether events are
  live or recorded.
- **Why:** replay is only useful if it reproduces live behaviour. A separate
  replay path silently becomes a different system.
- **Breaks if violated:** bugs reproduce in replay but not live (or worse, the
  reverse), and research results stop describing the live system. This also
  destroys the Phase 6 determinism guarantee and the future ML dataset with it.

### Correctness before performance

- **Rule:** no optimisation lands before the behaviour it touches has a passing
  test, and no optimisation lands without a benchmark showing it helped.
- **Why:** a fast incorrect order book is worse than a slow one — it produces
  plausible wrong numbers instead of obvious failures.
- **Breaks if violated:** silent data corruption in stored datasets, which then
  poisons every downstream analysis and the future generative model.

### Invalid state is loud, never silent

- **Rule:** on a sequence gap, crossed book, or failed validation, the book is
  marked invalid and stops serving reads until resynchronisation completes. It
  never continues on best-effort.
- **Why:** *known invalid* beats *unknown but silently incorrect*. The whole
  reliability posture rests on this.
- **Breaks if violated:** the system keeps emitting features from a corrupt
  book. Nothing alerts, the data looks fine, and the corruption is discovered
  weeks later in stored Parquet — unrecoverably.

### Analytics read the book, they don't mutate it

- **Rule:** microstructure computation is a pure function of book state plus
  trade flow. Feature code never writes to the book.
- **Why:** keeps features reproducible from stored state and makes analytics
  independently testable.
- **Breaks if violated:** replaying the same events stops producing the same
  features, and Phase 6 determinism fails.

### The research boundary exposes facts, not interpretations

- **Rule:** the research API serves recorded events exactly as stored, in
  replay order. It must not label, window, normalise, aggregate, or infer
  order-lifecycle events from depth changes.
- **Why:** a depth decrease could be a cancellation or a fill, and the exchange
  never says which. Choosing is a modelling assumption. Once TickForge makes it,
  every consumer inherits it invisibly and none can undo it.
- **Breaks if violated:** downstream models train on TickForge's assumptions
  believing them to be exchange facts, and a wrong assumption becomes
  unfalsifiable because the raw distinction was discarded before they saw it.

## Data flow

Conceptual stages — what each produces, not which module calls which.

```
raw exchange frame
   → (adapter)      normalized MarketEvent: BookSnapshot | BookUpdate | Trade | MarketStatus
   → (pipeline)     ordered, validated event stream
   → (book)         L2 book state + validity status
   → (analytics)    feature snapshot
   → (storage)      Parquet partitions
```

Storage sits downstream of the book so recorded data reflects validated state;
raw events are also persisted so history can be re-derived when book logic
changes.
