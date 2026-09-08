"""Run a live Binance feed and print microstructure features.

Manual smoke test for the whole pipeline -- adapter, resync state machine,
order book and analytics -- against the real exchange::

    uv run python -m tickforge
    uv run python -m tickforge ETHUSDT 60
    uv run python -m tickforge BTCUSDT 600 data     <- also record to Parquet

and replays one back through the identical pipeline::

    uv run python -m tickforge replay BTCUSDT 2026-09-06        as fast as possible
    uv run python -m tickforge replay BTCUSDT 2026-09-06 100    at 100x

Not a test: it needs the network and it prints rather than asserts. Its job is
to surface what only appears against a live venue -- whether microprice
actually leads price, whether imbalance moves before the touch does, and
whether any of it survives a real resync.

With a data directory it doubles as the capture tool. Recording tees the feed,
so what lands on disk is the event stream exactly as emitted -- before the book
or the analytics touch it, which is what makes it replayable.

`consume` is shared by both modes and cannot tell them apart. That is the
point: a separate replay path would silently become a different system.
"""

import asyncio
import datetime as dt
import sys
from collections.abc import AsyncIterator
from decimal import Decimal

from tickforge.adapters.binance_feed import BinanceFeed
from tickforge.analytics import FlowFeatures, feature_snapshot
from tickforge.book import ApplyResult, OrderBook
from tickforge.events import BookSnapshot, MarketEvent, Trade
from tickforge.replay import replay_partition
from tickforge.storage import EventStore, record

WINDOW_NS = 60 * 1_000_000_000


def show(value: Decimal | None, spec: str) -> str:
    """Format a feature, or a dash where it has nothing to report yet."""
    return "--".rjust(len(format(Decimal(0), spec))) if value is None else format(value, spec)


async def consume(
    symbol: str,
    source: AsyncIterator[MarketEvent],
    store: EventStore | None = None,
    duration_s: float | None = None,
) -> None:
    """Drive the book, the analytics and optionally storage from an event source.

    `source` is a live feed or a replay, and **nothing in here can tell which**.
    That is the architectural claim, demonstrated rather than asserted: if this
    function ever needed to branch on it, replay would have stopped being
    evidence about live behaviour -- see docs/knowledge/architecture.md.

    Args:
        duration_s: Wall-clock stop for a live run. None replays to the end of
            the recording. The only wall clock here, and it decides when to
            stop reading, never what any event means.
    """
    book = OrderBook("binance", symbol.upper())
    flow = FlowFeatures(WINDOW_NS)
    events = 0
    snapshots = 0
    trades = 0

    try:
        async with asyncio.timeout(duration_s):
            async for event in source:
                events += 1

                if isinstance(event, BookSnapshot):
                    snapshots += 1
                    state = book.load_snapshot(event)
                    if book.is_valid:  # a crossed snapshot loads INVALID
                        flow.observe(event, book)
                    print(
                        f"snapshot seq={event.last_seq} "
                        f"{len(event.bids)}x{len(event.asks)} levels -> {state.name}"
                    )
                    continue

                # Before the book branch: a trade reaching `book.apply` would
                # be an AttributeError on `last_seq`.
                if isinstance(event, Trade):
                    trades += 1
                    flow.observe(event, book)
                    continue

                result = book.apply(event)
                if result is not ApplyResult.APPLIED:
                    # Anything but APPLIED means the feed and the book
                    # disagree about what a valid sequence is.
                    print(f"!! {result.name} at seq {event.first_seq}-{event.last_seq}")
                    continue

                flow.observe(event, book)

                f = feature_snapshot(book, flow, event.timestamp_ns)
                if store is not None:
                    store.write_features(f)

                if f.midprice is None:
                    continue
                mid = f.midprice
                # Offsets and basis points: microprice tracks mid to within a
                # tick and realised vol runs ~1e-5, so absolutes print as noise.
                rv_bp = None if f.realised_volatility is None else f.realised_volatility * 10000
                vwap_offset = None if f.vwap is None else f.vwap - mid
                print(
                    f"mid {mid:.2f} "
                    f"micro {f.microprice - mid:+.4f} | "
                    f"L1 {f.imbalance_1:+.2f} "
                    f"L5 {f.imbalance_5:+.2f} | "
                    f"ofi {show(f.order_flow_imbalance, '+8.3f')} "
                    f"tImb {show(f.trade_imbalance, '+.2f')} "
                    f"vwap {show(vwap_offset, '+8.2f')} "
                    f"rv {show(rv_bp, '.2f')}bp"
                )
    except TimeoutError:
        pass

    print(
        f"\n{events} events, {trades} trade(s), {snapshots} snapshot(s) "
        f"--> more than one snapshot means a resync"
    )


async def watch(symbol: str, duration_s: float, data_root: str | None = None) -> None:
    """Consume a live Binance feed, optionally recording it."""
    store = None if data_root is None else EventStore(data_root, "binance", symbol.upper())
    feed = BinanceFeed(symbol)
    # Teed at the feed, so the recording is the raw emitted stream rather than
    # whatever survived the consuming loop's control flow.
    source = feed.run() if store is None else record(feed.run(), store)

    try:
        await consume(symbol, source, store, duration_s)
    finally:
        # Closes the Parquet footers. Without them the files hold rows nobody
        # can read, so this has to survive the timeout and a Ctrl-C alike.
        if store is not None:
            store.close()

    if store is not None:
        print(f"recorded to {data_root}/binance/{symbol.upper()}/")


async def rerun(symbol: str, date: str, speed: float, data_root: str) -> None:
    """Consume a recorded capture through the same pipeline a live feed uses."""
    source = replay_partition(data_root, "binance", symbol.upper(), date, speed)
    await consume(symbol, source)


def measure(symbol: str, date: str, data_root: str) -> None:
    """Profile the pipeline over a recorded capture.

    Reads the capture rather than the live feed so the measurement is
    reproducible: two runs process identical work, which is the difference
    between a benchmark and a stopwatch.
    """
    from tickforge import bench
    from tickforge.replay import read_partition

    events = read_partition(data_root, "binance", symbol.upper(), date)
    bench.report(events)
    bench.storage_throughput(events, _scratch())
    bench._decimal_cost()


def _scratch() -> str:
    """A throwaway directory for the storage benchmark's output."""
    import tempfile

    return tempfile.mkdtemp(prefix="tickforge-bench-")


def main() -> None:
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "bench":
            symbol = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
            date = sys.argv[3] if len(sys.argv) > 3 else _today()
            root = sys.argv[4] if len(sys.argv) > 4 else "data"
            measure(symbol, date, root)
        elif len(sys.argv) > 1 and sys.argv[1] == "replay":
            symbol = sys.argv[2] if len(sys.argv) > 2 else "BTCUSDT"
            date = sys.argv[3] if len(sys.argv) > 3 else _today()
            speed = float(sys.argv[4]) if len(sys.argv) > 4 else 0.0
            root = sys.argv[5] if len(sys.argv) > 5 else "data"
            asyncio.run(rerun(symbol, date, speed, root))
        else:
            symbol = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"
            duration_s = float(sys.argv[2]) if len(sys.argv) > 2 else 30.0
            data_root = sys.argv[3] if len(sys.argv) > 3 else None
            asyncio.run(watch(symbol, duration_s, data_root))
    except KeyboardInterrupt:
        pass
    except FileNotFoundError as missing:
        print(missing)


def _today() -> str:
    return dt.datetime.now(tz=dt.UTC).strftime("%Y-%m-%d")


if __name__ == "__main__":
    main()
