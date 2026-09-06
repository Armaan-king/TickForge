# Deterministic Replay — Design

**Date:** 2026-09-06
**Phase:** 6 (Deterministic Historical Replay)
**Status:** approved

## Purpose

Read a recorded capture back into `MarketEvent`s and inject them at the same
seam `BinanceFeed.run()` feeds, so live and recorded data traverse one
pipeline. Two replays of the same capture must produce identical output.

This is the project's central architectural claim. Until now it has been an
assertion in `architecture.md`; this phase makes it testable.

## What the measurements forced

Two findings from probing the existing captures changed the design before any
replay code was written. Both are recorded because a future reader would
otherwise reasonably undo the fixes.

### Neither timestamp column can order the stream

```
time.time_ns() granularity on the dev machine:  600,500 ns
                       9 distinct values in 200,000 samples

73 captured events:  2 duplicate received_ns,  2 duplicate timestamp_ns
sorting by received_ns == sorting by timestamp_ns?   False
```

Windows resolves `time.time_ns()` coarsely, so roughly 3% of events share a
`received_ns`. Ties make the merge order arbitrary, which defeats the entire
guarantee.

The two columns also disagree. A captured trade:

```
timestamp_ns  1788682770058000000    Binance's match time
received_ns   1788682769871062000    our clock, 187 ms EARLIER
```

The exchange clock leads this machine, so an event's own timestamp can precede
its arrival. `pitfalls.md` warns that venue timestamps are neither trustworthy
nor uniform; this is that warning with a number attached.

**Consequence:** an explicit `capture_seq` column, assigned by the store at
write time, monotonic across all three raw streams within a session. Named to
stay distinct from `first_seq`/`last_seq`, which are the venue's sequence
numbers and mean something else entirely.

### A second capture destroyed the first

```
after session 1:  3 rows  [1, 2, 3]
after session 2:  2 rows  [4, 5]
```

`pq.ParquetWriter` opens for writing, not appending. Two captures on one day
meant the second silently replaced the first — a data-loss bug shipped in
Phase 5, surfaced only by asking how replay would read two sessions.

**Consequence:** one file per session. `EventStore` takes a session stamp and
writes `trades-<stamp>.parquet`. Overwriting becomes structurally impossible
rather than merely discouraged.

Rejected — refuse to open an occupied partition: a capture that crashes at
minute 40 would block the rest of the day until cleared by hand.

Rejected — read and rewrite merged: a full read-modify-write of a multi-GB
file at every startup, and a crash mid-rewrite loses both captures instead of
one.

This departs from the single-filename layout `Goal.md` sketches. Parquet
readers already treat a directory as one dataset — `pq.read_table(dir)` and
`polars.scan_parquet` both glob — so nothing downstream is harmed.

## Ordering

Events are ordered by `(session, capture_seq)`. Sessions sort by their
filename stamp; within a session `capture_seq` is a dense counter. No
timestamp participates in ordering.

## Reading

Whole-partition read into memory, then merge. A day of BTCUSDT is roughly 3M
trades — large but tractable, and this is a 6-hour phase.

Rejected for now — a streaming heap merge across three lazily-read files:
constant memory and no length limit, but real work, and Phase 9 is where
storage throughput belongs. Marked as the upgrade path in the code.

Rejected — merging the three streams into one file at write time: replay
becomes trivial, but one schema for three event shapes means mostly-null
columns and the columnar advantage is gone.

## Pacing

`speed=0` replays as fast as possible. Otherwise each event waits the
`received_ns` delta from its predecessor, divided by speed — the arrival
rhythm, which is what the live experience actually was, rather than the
exchange-stamped cadence with its clock skew.

Wall clock is used **only** here. `pitfalls.md` permits it explicitly for
pacing: it changes when output appears, never what the output is. Negative
deltas clamp to zero, since `time.time_ns()` can step backwards.

## Interface

```
src/tickforge/replay.py

_events_from(table, stream) -> list[MarketEvent]   reverse of EventStore._row
_session_files(directory) -> dict[str, list[Path]] a partition grouped by session
read_partition(root, exchange, symbol, date) -> list[MarketEvent]
replay(events, speed=0) -> AsyncIterator[MarketEvent]
```

`replay` yields at the same seam `BinanceFeed.run()` does, so the consuming
loop is unchanged. That the CLI needs no branch on live-vs-replay is the
architectural claim, demonstrated rather than asserted.

## CLI

```
python -m tickforge BTCUSDT 600 data                live capture
python -m tickforge replay BTCUSDT 2026-09-06       replay, unpaced
python -m tickforge replay BTCUSDT 2026-09-06 100   replay at 100x
```

Dispatch on `argv[1] == "replay"`. No argparse; the positional style still
fits, and adding one is churn this phase does not need.

## Testing

- **Determinism.** Replay the same capture twice, collect every
  `FeatureSnapshot`, assert the lists are equal. This is success criterion 6.
- **Round trip.** Events written by `EventStore` come back identical --
  every field, with `Decimal` equality, not approximate.
- **Ordering.** A capture whose `received_ns` values collide still replays in
  `capture_seq` order. This is the test the measurements above exist for.
- **Multi-session.** Two sessions in one partition both survive and replay in
  session order.
- **Pacing.** `speed=0` does not sleep; a positive speed sleeps proportionally
  and a negative delta does not sleep at all.
- **Same seam.** A book and analytics driven by replay reach the same state as
  the same events applied directly.

## Out of scope

- Streaming merge for captures larger than memory — Phase 9.
- Replaying a date *range* rather than a single day. One more loop; add it
  when something needs it.
- Re-recording a replay into a new capture. It would work, but nothing needs
  it and the determinism test covers what it would prove.
