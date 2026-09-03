# Decisions

Newest first. One entry per decision that would otherwise get re-argued in six
months. Record the rejected options too — that's what stops the re-argument.

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
