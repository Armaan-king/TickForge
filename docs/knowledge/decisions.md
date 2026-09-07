# Decisions

Newest first. One entry per decision that would otherwise get re-argued in six
months. Record the rejected options too — that's what stops the re-argument.

---

## 2026-09-07 — `Decimal` stays; the 20× claim was wrong

**Decision:** keep `decimal.Decimal` for prices and quantities. The Phase 9
benchmark that was supposed to justify switching to scaled `int` instead
closed the question the other way.

**Measured** (`python -m tickforge bench`, call overhead subtracted):

```
Decimal multiply   27.0 ns
int multiply       24.4 ns
ratio               1.1x
```

The 2026-09-02 entry below estimated 20×. It is wrong by a factor of eighteen.

**Why:** in Python the alternative is not a machine-word integer. A price at
8dp scaled to an int is ~7.7e12, a quantity ~1.3e8, and their product ~1e21 —
past 64 bits, so CPython falls back to multi-digit arithmetic. `Decimal`'s C
backend (libmpdec) is competitive with that. **The scaled-int trick buys in
C++ what it does not buy in Python**, because Python has no machine-word
integer type to win with.

**Consequence:** the largest deferred performance question in the project is
closed, and the exactness guarantee costs almost nothing. Do not re-propose
scaled ints without a measurement showing something different — and note that
a benchmark comparing `Decimal` against *small* ints would mislead, since
those are not the numbers this system holds.

---

## 2026-09-07 — Feature computation is the dominant cost, and stays that way

**Decision:** leave `feature_snapshot` at ~374 µs per call. Measured, named,
not optimised.

**Why:** it reaches `top_bids`/`top_asks`/`best_bid` roughly ten times per
row, each a full pass over a 1000-level side. Computing `top_bids(10)` once
and slicing it for depths 1 and 5 would cut it to roughly 100 µs.

But features are computed once per *book update*, and Binance pushes depth
every 1000 ms. That is 374 µs per second — 0.04% of one core. The optimisation
would be real and would buy nothing.

**Rejected — do it anyway because it is easy:** it needs level-taking variants
of five pure functions, or inlined arithmetic that duplicates the formulas.
The second is how microprice's crossed weighting gets "fixed" into a bug.

**Consequence:** subscribing to `@depth@100ms` multiplies this by ten and it is
still 0.4% of a core, so the ceiling is far off. Revisit if a venue pushes
depth faster than that, or if feature rows are ever computed per *trade*
rather than per update — at 33 trades/sec that would be 1.2% and climbing.

---

## 2026-09-05 — `Trade` carries a derived aggressor and the match time

**Decision:** `Trade.aggressor` is a `Side` computed in the adapter, not
Binance's `m` flag stored verbatim. `Trade.timestamp_ns` is the venue's match
time (`T`), not its emission time (`E`). `Side` is a `StrEnum`.

**Why:** `m` answers "was the buyer the market maker?" — a Binance sentence,
and a *negation* of the thing analytics actually want. Storing it would put a
venue's field semantics downstream of the adapter, and every consumer would
re-derive the inversion, each with its own chance of getting it backwards.
`T` over `E` because the gap between them is server-side queuing: real, but
not market information, and Phase 4 correlates trades against book state by
this timestamp. `StrEnum` because `Side` gets written to Parquet in Phase 5 —
a member *is* its string, so it round-trips through storage with no conversion
layer, unlike the `Enum`/`auto()` pattern used for the book's internal states.

**Rejected — store `is_buyer_maker: bool`:** lossless and venue-shaped. Pushes
a boolean whose meaning is inverted relative to the question being asked into
every downstream consumer.

**Rejected — `E` as the timestamp:** consistent with `BookUpdate`, and wrong
for a different reason: a depth frame has no moment of occurrence separate
from its emission, and a trade does.

**Consequence:** a venue that reports the aggressor directly needs no
inversion, and one that reports neither cannot produce a `Trade` at all —
`aggressor` is not optional. The sign convention is now asserted in
`test_binance_adapter.py` in both directions, because a flipped aggressor
produces plausible order-flow imbalance rather than an obvious failure.

---

## 2026-09-05 — Depth and trades share one combined socket

**Decision:** `stream_events` subscribes to `@depth` and `@trade` through
Binance's combined-stream endpoint, yielding both types from one connection.

**Why:** the interleaving of trades and book updates is market information.
Phase 4 reads trade flow against book state, so the order has to come from the
venue rather than from whichever `asyncio` task the loop happened to schedule
first. One socket also means one reconnect, one resync, and one place where
the sequencing state lives.

**Rejected — two connections merged with an `asyncio.Queue`:** invents an
ordering the exchange never stated, and makes it depend on scheduling — so
replay of a recorded session would not reproduce the live interleaving.

**Rejected — trades on a separate feed object entirely:** the book and trade
flow would resync independently, and analytics could see trades from a window
the book never covered.

**Consequence:** the seeding buffer is now mixed. Trades captured during
seeding are emitted as a block after the snapshot rather than in arrival
order, which is unobservable downstream — a trade has no sequence number to
be ordered against — but is a real, if bounded, reordering. Adding a third
subscription means touching the dispatch in `parse_stream_frame` and nothing
else.

---

## 2026-09-02 — `apply()` returns a result; reads fail closed

**Decision:** `OrderBook.apply()` returns an `ApplyResult` rather than raising
on a sequence gap. Independently, every read (`best_bid`, `best_ask`) raises
`BookInvalidError` while the book is out of sync.

**Why:** two guards that do not depend on each other. Gaps are a *normal*
operating condition per Phase 7, so raising would mean exceptions for ordinary
control flow. But a returned status the caller ignores is exactly the silent
corruption the project exists to avoid — so ignoring it costs an error at read
time instead of plausible-looking wrong numbers.

**Rejected — raise on gap:** makes every call site a try/except for something
that happens routinely.

**Rejected — status only, reads always served:** one forgotten `if` and the
book serves numbers from a state it knows is wrong.

**Consequence:** callers must handle `BookInvalidError`, including the Phase 10
API, which needs to translate it into a meaningful status rather than a 500.

---

## 2026-09-02 — Prices and quantities are `Decimal`

**Decision:** `BookUpdate` carries price and quantity as `decimal.Decimal`.
Events are `frozen=True, slots=True`, with level lists as tuples.

**Why:** exchanges send prices as decimal *strings* (`"77381.36000000"`)
specifically so the exact value survives — JSON numbers are IEEE-754 doubles
and cannot represent most tick sizes. Price is a dictionary key in the order
book, so an inexact type means levels that should match sometimes don't, and
levels that should delete sometimes linger. Correctness before performance.

**Rejected — `float`:** fastest and wrong. `0.1 + 0.2 != 0.3` becomes a
phantom price level, and the failure is silent.

**Rejected — scaled `int` (price × 10⁸):** exact *and* fast, and what
production systems use. Rejected only for now: it requires tracking a scale
factor per symbol and converting at every boundary, which is complexity bought
before any measurement justified it.

**Rejected — keeping the wire `str`:** lossless but useless — imbalance and
VWAP need arithmetic.

**Consequence:** `Decimal` is roughly 20× slower than int arithmetic and
heavier in memory. This is the headline candidate for Phase 9: benchmark
`Decimal` against scaled `int` and switch on evidence. Until then the
representation is deliberately the slow, correct one.

---

## 2026-09-02 — Scope: market-data infrastructure, not a matching engine

**Decision:** TickForge reconstructs and analyses market data. It does not
match orders, simulate an exchange, or chase microsecond latency.

**Why:** the skills being demonstrated are market-data engineering, async
Python, reliability and research tooling. An ultra-low-latency matching engine
is a different project with a different language.

**Rejected — competing with C++ low-latency infrastructure:** unwinnable in
Python and not the point. The interesting question is how far a
well-engineered Python system goes for *data processing and research*.

**Consequence:** latency targets are "fast enough to keep up with the feed and
measurable", not "lowest possible". Performance work is benchmark-driven, not
aspirational.

---

## 2026-09-02 — Exchange logic isolated behind adapters

**Decision:** each venue gets an adapter that translates its wire format into
normalized `MarketEvent`s. Nothing downstream knows the venue exists.

**Why:** multi-venue support, testability without a live connection, and
replay all depend on downstream code being venue-agnostic.

**Rejected — exchange-specific handling throughout the pipeline:** faster for
the first exchange, then every subsequent venue is a system-wide refactor.

**Consequence:** the normalized event model must be rich enough for every
venue's semantics. Where venues genuinely differ (sequencing schemes,
resync procedures), the difference is absorbed *inside* the adapter even when
that makes the adapter ugly.

---

## 2026-09-02 — Live and replay share one pipeline

**Decision:** recorded events re-enter at the same seam as live events. No
component branches on live-vs-replay.

**Why:** replay's entire value is behavioural fidelity. Two code paths means
two systems that drift.

**Rejected — a dedicated replay engine:** simpler to write, but then replay
stops being evidence about live behaviour, which is the only reason it exists.

**Consequence:** the pipeline must not depend on wall-clock time for logic —
only for pacing. Any wall-clock dependency breaks deterministic replay.

---

## 2026-09-02 — Invalid book state halts rather than degrades

**Decision:** sequence gap, crossed book or failed validation marks the book
invalid. It stops serving until resynchronisation succeeds.

**Why:** *known invalid* beats *unknown but silently incorrect*. A corrupt book
that keeps serving produces plausible numbers that nothing flags.

**Rejected — best-effort continuation through gaps:** keeps uptime metrics
pretty while quietly poisoning stored datasets and every downstream analysis.

**Consequence:** the system will have visible downtime during resync. That is
the intended trade — availability is sacrificed for correctness.

---

## 2026-09-02 — Knowledge base is curated markdown, not a structured graph

**Decision:** `docs/knowledge/` stores only what the source code cannot
explain — design reasons, architectural boundaries, domain knowledge, non-obvious
interactions. Plain markdown, modular, no machine-readable index.

**Why:** The dividing line is *derivable vs. not*. Anything derivable from the
source (imports, call graphs, signatures, "what does this change affect") is
answered correctly and instantly by `rg` and the compiler. Recording it by hand
produces a lower-fidelity second copy that drifts. Reasons and boundaries don't
drift when files move, so they're safe to write down.

**Rejected — YAML dependency graph (`relationships.yaml`):** duplicates the
import graph. Its failure mode is the dangerous one: an agent reads a stale
relationship as ground truth and *skips* the grep that would have caught it.
Wrong architectural memory is worse than none.

**Rejected — YAML index file:** a list of concept files that `ls` already
produces, with the added property of being able to go stale.

**Rejected — single growing `Project_Knowledge.md`:** forces a full read for
one fact, and grows until nobody maintains it.

**Consequence:** the knowledge base is only as good as its update discipline.
The trigger is surprise (see `INDEX.md`), chosen because it self-detects where
"update on significant change" does not.

---

## YYYY-MM-DD — _Template, copy this_

**Decision:** _what was decided_

**Why:** _the reasoning that would otherwise be lost_

**Rejected:** _the alternatives, and what was wrong with each_

**Consequence:** _what this costs us, or what it now constrains_
