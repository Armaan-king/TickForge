"""Measured pipeline performance, driven from a recorded capture.

Phase 9 begins only once the system is correct, and every number here is
measured rather than asserted -- `Goal.md`'s "Measured Performance" principle
says performance claims need reproducible benchmarks, and this is what makes
them reproducible: it replays a real capture, so two runs measure the same
work on the same data.

Timing uses `time.perf_counter_ns`, never `time.time_ns`. The latter resolves
to ~600us on Windows, which is coarser than most of what is being measured --
`_date_of` in storage.py exists partly because of that same limitation.
"""

import time
import tracemalloc
from dataclasses import dataclass
from decimal import Decimal

from tickforge.analytics import FlowFeatures, feature_snapshot
from tickforge.book import ApplyResult, OrderBook
from tickforge.events import BookSnapshot, BookUpdate, MarketEvent

WINDOW_NS = 60 * 1_000_000_000


@dataclass(frozen=True, slots=True)
class Latency:
    """One stage's timing distribution, in microseconds.

    Percentiles rather than a mean: a pipeline that averages 20us but stalls
    for 8ms once a second drops frames, and the mean hides it entirely.
    """

    stage: str
    count: int
    p50: float
    p95: float
    p99: float
    worst: float

    def __str__(self) -> str:
        return (
            f"{self.stage:<16} {self.count:>7}  "
            f"p50 {self.p50:>8.1f}  p95 {self.p95:>8.1f}  "
            f"p99 {self.p99:>8.1f}  max {self.worst:>9.1f}"
        )


def _latency(stage: str, samples_ns: list[int]) -> Latency:
    """Summarise raw nanosecond samples.

    Indexes a sorted list rather than interpolating: with thousands of samples
    the difference is below the measurement noise, and an exact sample is a
    real observation rather than a number between two of them.
    """
    if not samples_ns:
        return Latency(stage, 0, 0.0, 0.0, 0.0, 0.0)
    ordered = sorted(samples_ns)
    last = len(ordered) - 1

    def at(fraction: float) -> float:
        return ordered[min(last, int(len(ordered) * fraction))] / 1_000

    return Latency(stage, len(ordered), at(0.50), at(0.95), at(0.99), ordered[-1] / 1_000)


def profile(
    events: list[MarketEvent], trace: bool = False
) -> tuple[list[Latency], float, int]:
    """Drive the book and analytics over `events`, timing each stage.

    Mirrors what `__main__.consume` does minus the printing, so the numbers
    describe the pipeline a user actually runs.

    Args:
        trace: Measure peak memory. Off by default because `tracemalloc`
            instruments every allocation and roughly doubles the latencies --
            a benchmark that inflates its own headline number is worse than no
            benchmark. `report` runs the loop twice instead: once for timing,
            once for memory.

    Returns:
        ``(latencies, wall_seconds, peak_bytes)``. Peak memory covers the
        whole process, and a capture read into memory dominates it -- read it
        as "what replaying this costs", not "what the book costs".
    """
    book = OrderBook("binance", "BTCUSDT")
    flow = FlowFeatures(WINDOW_NS)
    samples: dict[str, list[int]] = {"book": [], "flow": [], "features": [], "total": []}

    if trace:
        tracemalloc.start()
    started = time.perf_counter()

    for event in events:
        event_start = time.perf_counter_ns()

        mark = time.perf_counter_ns()
        if isinstance(event, BookSnapshot):
            book.load_snapshot(event)
            applied = book.is_valid
        elif isinstance(event, BookUpdate):
            applied = book.apply(event) is ApplyResult.APPLIED
        else:
            applied = True
        samples["book"].append(time.perf_counter_ns() - mark)
        if not applied:
            continue

        mark = time.perf_counter_ns()
        flow.observe(event, book)
        samples["flow"].append(time.perf_counter_ns() - mark)

        if isinstance(event, BookUpdate):
            mark = time.perf_counter_ns()
            feature_snapshot(book, flow, event.timestamp_ns)
            samples["features"].append(time.perf_counter_ns() - mark)

        samples["total"].append(time.perf_counter_ns() - event_start)

    wall = time.perf_counter() - started
    peak = 0
    if trace:
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

    return [_latency(stage, taken) for stage, taken in samples.items()], wall, peak


def report(events: list[MarketEvent]) -> None:
    """Print a profile of one capture. Numbers only -- no pass or fail.

    A benchmark that asserts a threshold fails on a busy laptop and teaches
    nothing. This exists to be read, and to be re-run after a change.
    """
    latencies, wall, _ = profile(events)
    # A second pass purely for memory, because tracing allocations distorts
    # the timings it would otherwise be sharing a run with.
    _, _, peak = profile(events, trace=True)

    print(f"{len(events)} events in {wall:.3f}s")
    print(f"throughput   {len(events) / wall:>12,.0f} events/sec")
    print(f"peak memory  {peak / 1024 / 1024:>12,.1f} MiB")
    print()
    print(f"{'stage':<16} {'count':>7}  {'microseconds':^52}")
    for row in latencies:
        print(row)


def storage_throughput(events: list[MarketEvent], root) -> None:
    """Time writing a capture to Parquet, and measure what it costs on disk."""
    from pathlib import Path

    from tickforge.storage import EventStore

    root = Path(root)
    started = time.perf_counter()
    with EventStore(root, "binance", "BTCUSDT", session=1) as store:
        for event in events:
            store.write(event)
    wall = time.perf_counter() - started

    written = sum(p.stat().st_size for p in root.rglob("*.parquet"))
    print()
    print(f"storage      {len(events) / wall:>12,.0f} events/sec written")
    print(f"             {written / len(events):>12,.1f} bytes/event on disk")
    print(f"             {written / 1024:>12,.1f} KiB total")


def _decimal_cost() -> None:
    """The headline Phase 9 question `decisions.md` left open.

    `Decimal` was chosen over scaled `int` for exactness, with the cost
    explicitly deferred to a measurement. This is that measurement.

    An empty callable is timed and subtracted, because the loop and call
    overhead is comparable to the arithmetic itself -- leaving it in would
    inflate both sides equally and squash the ratio toward 1, which is exactly
    the kind of measurement that talks you out of a real cost.
    """
    a, b = Decimal("77381.36000000"), Decimal("1.29993000")
    x, y = 7738136000000, 129993000

    def timed(fn, rounds=500_000):
        started = time.perf_counter_ns()
        for _ in range(rounds):
            fn()
        return (time.perf_counter_ns() - started) / rounds

    overhead = timed(lambda: None)
    dec = timed(lambda: a * b) - overhead
    integer = timed(lambda: x * y) - overhead

    print()
    print(f"call overhead    {overhead:>8.1f} ns  (subtracted from both)")
    print(f"Decimal multiply {dec:>8.1f} ns")
    print(f"int multiply     {integer:>8.1f} ns")
    print(f"ratio            {dec / integer:>8.1f}x")
