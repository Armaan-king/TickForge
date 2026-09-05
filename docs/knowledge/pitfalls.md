# Pitfalls

Cross-cutting traps — ones that bite regardless of which component you're in.

**Component-specific gotchas belong in that component's `concepts/` file.**
Keeping the split sharp is what stops both files from being half-full.

The entries below are *known domain hazards*, drawn from the spec rather than
from experience. Anything learned the hard way should be added with what
actually happened.

## Float arithmetic on prices and quantities

- **Symptom:** price levels that should be equal compare unequal; levels fail
  to delete; quantities drift toward tiny non-zero residue instead of zero.
- **Cause:** binary floats can't represent decimal tick sizes exactly, and
  exchanges send prices as decimal strings.
- **Avoid by:** deciding a single representation early (integer ticks, or
  `Decimal`, or scaled ints) and never using a float as a dict key or an
  equality target. Record the choice in `decisions.md` when made.

## Wall-clock time anywhere in pipeline logic

- **Symptom:** replay produces different output on different runs, or differs
  from the live run it recorded.
- **Cause:** any use of `time.time()`, `datetime.now()`, or timeouts measured
  against real time inside processing logic.
- **Avoid by:** logic uses event timestamps only. Wall-clock is permitted for
  *pacing* replay and for connection health, never for decisions that affect
  output. This is what makes Phase 6 determinism possible.

## Exchange timestamps are not trustworthy or uniform

- **Symptom:** events appear out of order, timestamps go backwards, or
  precision silently differs between venues.
- **Cause:** venues stamp at different points, with different clock quality,
  in different units (ms, µs, ns) — and some fields are *receive* time, not
  *event* time.
- **Avoid by:** normalizing to `timestamp_ns` in the adapter, recording both
  exchange time and local receive time, and ordering on sequence numbers where
  the venue provides them.

## Snapshot/update race on subscribe

- **Symptom:** book is subtly wrong from the start, often only detectable
  hours later as accumulated drift.
- **Cause:** the classic trap — fetching a REST snapshot while the WebSocket
  stream is already running requires buffering updates and discarding those
  older than the snapshot, per each venue's documented procedure.
- **Avoid by:** implementing the venue's stated resync procedure exactly, and
  testing it. Never assume the generic approach works for a new venue.
- **Binance specifically:** open the stream and buffer *before* fetching the
  snapshot. Discard buffered updates with `last_seq <= snapshot.last_seq`. The
  first update applied must **span** the boundary — `first_seq <=
  snapshot.last_seq + 1 <= last_seq` — because the snapshot lands partway
  through an update's range. Requiring exact contiguity there rejects the
  first legitimate update and resyncs forever. `OrderBook` models this as the
  `SEEDED` state, distinct from `SYNCED`.

## A feed laxer than the book it feeds

- **Symptom:** the book goes INVALID and stays there forever. The feed keeps
  streaming, the process looks healthy, and nothing alerts.
- **Cause:** two sequence checks with different strictness. The book's check
  fails closed; the feed's triggers recovery. The book has no channel back to
  the feed, and the feed is the only thing that can resynchronise — so an
  update the feed accepts and the book rejects kills the book permanently, with
  no one to notice. An *overlapping* update is the usual culprit: it is not a
  gap, so a `>` comparison lets it through.
- **Avoid by:** making the feed's check exactly the book's, mode for mode — an
  update immediately after a snapshot may span the boundary, every one after
  that must be exactly contiguous. Anything the book would reject must raise in
  the feed instead. Learned the hard way: `_require_contiguous` and
  `OrderBook.apply` mirror each other on purpose, and drifting apart is the
  failure mode.
- **General form:** wherever a producer and a validator both check the same
  invariant, the producer must be the stricter of the two.

## Async generators are not closed by abandoning them

- **Symptom:** sockets stay open after a resync; connection-limit errors after
  hours of running; `async generator ignored GeneratorExit` at interpreter exit.
- **Cause:** `break`ing out of an `async for`, or unwinding past it, leaves the
  generator *suspended*, not closed. Its `finally` — which is where the `async
  with` holding the socket lives — runs only on `aclose()`, or at garbage
  collection, which may never happen once the loop is closing.
- **Avoid by:** any code iterating an async generator that owns a resource
  closes it in its own `finally`. This does not nest automatically: closing an
  outer generator does **not** close the inner one it was iterating, so each
  level needs its own `aclose()`. Both levels of `BinanceFeed` need one.

## Silent exception swallowing in async tasks

- **Symptom:** ingestion "works" but data quietly stops arriving; no error
  anywhere.
- **Cause:** an exception in a fire-and-forget `asyncio` task with no reference
  held, or a bare `except` in a reconnect loop.
- **Avoid by:** holding references to created tasks, and making failures loud.
  Connection health must be observable, not inferred from data still flowing.
