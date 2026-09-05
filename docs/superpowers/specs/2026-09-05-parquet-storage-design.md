# Parquet Event Storage — Design

**Date:** 2026-09-05
**Phase:** 5 (Historical Market Data Storage)
**Status:** approved

## Purpose

Persist the normalized `MarketEvent` stream to date-partitioned Parquet so that
history can be re-derived when book or analytics logic changes, and so Phase 6
can replay a recorded session through the same pipeline that produced it.

Raw events only in this increment. `features.parquet` follows once a
`FeatureSnapshot` type exists; the raw stream is the prerequisite, since
features can always be recomputed from it and never the reverse.

## Layout

```
data/binance/BTCUSDT/2026-09-05/trades.parquet
                               /book_updates.parquet
                               /snapshots.parquet
```

Snapshots get their own file rather than sharing `book_updates`. A snapshot
carries no sequence *range* and replaces the book wholesale; folding it in
would need nullable range columns and make a row's meaning depend on which
columns happen to be null. Replay also cannot start without one, so it is
addressed directly rather than filtered out of a mixed file.

`Goal.md` shows three files and describes the layout as "a possible dataset
layout", so this is an extension of that example rather than a departure.

## Schemas

Shared level type:

```
LEVEL  = struct<price: decimal128(38,18), quantity: decimal128(38,18)>
LEVELS = list<LEVEL>
```

| trades | book_updates | snapshots |
| --- | --- | --- |
| exchange: string | exchange: string | exchange: string |
| symbol: string | symbol: string | symbol: string |
| timestamp_ns: int64 | timestamp_ns: int64 | timestamp_ns: int64 |
| received_ns: int64 | received_ns: int64 | received_ns: int64 |
| trade_id: int64 | first_seq: int64 | last_seq: int64 |
| price: decimal128(38,18) | last_seq: int64 | bids: LEVELS |
| quantity: decimal128(38,18) | bids: LEVELS | asks: LEVELS |
| aggressor: string | asks: LEVELS | |

### Why these types

**`decimal128(38,18)` for price and quantity, not float.** `decisions.md`
records why this project uses `Decimal` throughout: exchanges send decimal
strings precisely because IEEE-754 cannot represent tick sizes, and price is a
dictionary key in the order book. Writing doubles would discard that guarantee
at the exact moment the data becomes permanent and irrecoverable.

**`int64` nanoseconds, not `timestamp('ns')`.** The events already carry
integer nanoseconds. The Parquet timestamp type adds timezone semantics and a
conversion that can only lose information; nothing downstream wants a
timezone-aware datetime, and the integers compare and subtract directly.

**One row per update, levels nested.** Preserves the event boundary exactly,
which replay requires — an update is atomic and its sequence range describes
the whole batch. Exploding to one row per level would multiply row count by
roughly 500 and force every reader to reconstruct update boundaries before it
could do anything.

**`exchange` and `symbol` as columns despite being in the path.** They look
redundant. Parquet dictionary-encodes a constant string column to one
dictionary entry plus RLE indices, so the cost is negligible, and it keeps a
file self-describing if it is ever moved out of its directory.

## Batching

Buffer rows per stream. Flush when **either** 5,000 rows have accumulated
**or** 30 seconds of event time have passed since the last flush. Each flush
becomes one Parquet row group.

Row count alone leaves a half-full buffer unwritten indefinitely on a quiet
symbol — hours lost to a crash. An interval alone gives wildly uneven row
groups on a busy one. Both bounds together keep the loss window small
regardless of feed rate.

Writing per event was rejected: every write would become its own row group
with full footer overhead, bloating files and collapsing throughput.

## Partition rollover

The partition date comes from `event.timestamp_ns`, never the wall clock.
Replaying last week's capture must write into last week's folder, not today's.
This is the third place the event-time rule applies, after the flow window and
the book's sequencing.

On a date change: flush all buffers, close all writers, open the new directory.

## Lifecycle

`EventStore` is a context manager. `close()` flushes and closes the Parquet
footers — an unclosed Parquet file has no footer and is unreadable, so this is
a correctness requirement rather than tidiness.

## Attachment to the pipeline

A `record()` async generator tees the feed:

```python
async for event in record(feed.run(), store):
    ...
```

Chosen over an explicit `store.write(event)` in each consumer because it makes
the recorded stream *definitionally* identical to the emitted stream — same
events, same order, nothing dropped by a caller's control flow. That is the
exact property Phase 6 must reproduce; with explicit calls it is a convention
each consumer has to honour, and a file cannot show whether they did.

Also testable against a fake async iterator: no feed, no network, no book.

A `Pipeline` object owning feed → book → analytics → storage was rejected as
premature. Exactly one consumer exists; extract it when replay makes the shared
shape real.

## Interface

```
_date_of(timestamp_ns) -> str            UTC date string for the partition path
_levels(levels) -> list[dict]            PriceLevel tuples -> Parquet structs
EventStore(root, exchange, symbol, batch_rows=5000, batch_ns=30e9)
EventStore.write(event) -> None          route by type, buffer, flush if due
EventStore.flush() -> None               buffered rows -> row groups
EventStore.close() -> None               flush + close footers
record(events, store) -> AsyncIterator   the tee
```

## Testing

- **Round-trip fidelity.** Write events, read back, assert `Decimal`s are
  *exactly* equal. Approximate equality would defeat the reason for the type.
- **Partition rollover** on an event-time date change, including that the old
  file is closed and readable.
- **Flush on row count** and **flush on event-time interval**, independently.
- **`record` yields everything it writes**, in order, unmodified.
- **Unclosed store is unreadable / closed store is readable** — makes the
  footer requirement explicit rather than folklore.

## Out of scope

- `features.parquet` — needs a `FeatureSnapshot` type; next increment.
- Reading for replay — Phase 6.
- Compression tuning, storage-size and throughput measurement — Phase 9,
  which is where benchmarks belong.
