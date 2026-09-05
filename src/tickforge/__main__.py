"""Run a live Binance feed and print microstructure features.

Manual smoke test for the whole pipeline -- adapter, resync state machine,
order book and analytics -- against the real exchange::

    uv run python -m tickforge
    uv run python -m tickforge ETHUSDT 60
    uv run python -m tickforge BTCUSDT 600 data     <- also record to Parquet

Not a test: it needs the network and it prints rather than asserts. Its job is
to surface what only appears against a live venue -- whether microprice
actually leads price, whether imbalance moves before the touch does, and
whether any of it survives a real resync.

With a data directory it doubles as the capture tool. Recording tees the feed,
so what lands on disk is the event stream exactly as emitted -- before the book
or the analytics touch it, which is what makes it replayable.
"""

import asyncio
import sys
from decimal import Decimal

from tickforge.adapters.binance_feed import BinanceFeed
from tickforge.analytics import FlowFeatures, feature_snapshot
from tickforge.book import ApplyResult, OrderBook
from tickforge.events import BookSnapshot, Trade
from tickforge.storage import EventStore, record

WINDOW_NS = 60 * 1_000_000_000


def show(value: Decimal | None, spec: str) -> str:
    """Format a feature, or a dash where it has nothing to report yet."""
    return "--".rjust(len(format(Decimal(0), spec))) if value is None else format(value, spec)


async def watch(symbol: str, duration_s: float, data_root: str | None = None) -> None:
    book = OrderBook("binance", symbol.upper())
    flow = FlowFeatures(WINDOW_NS)
    feed = BinanceFeed(symbol)
    events = 0
    snapshots = 0
    trades = 0

    store = None if data_root is None else EventStore(data_root, "binance", symbol.upper())
    # Teed at the feed, so the recording is the raw emitted stream rather than
    # whatever survived this loop's control flow.
    source = feed.run() if store is None else record(feed.run(), store)

    try:
        async with asyncio.timeout(duration_s):
            async for event in source:
                events += 1

                if isinstance(event, BookSnapshot):
                    snapshots += 1
                    state = book.load_snapshot(event)
                    # Only observe a book the load actually validated -- a
                    # crossed snapshot leaves it INVALID and its reads raise.
                    if book.is_valid:
                        flow.observe(event, book)
                    print(
                        f"snapshot seq={event.last_seq} "
                        f"{len(event.bids)}x{len(event.asks)} levels -> {state.name}"
                    )
                    continue

                # Must come before the book branch: a trade reaching
                # `book.apply` would be an AttributeError on `last_seq`.
                if isinstance(event, Trade):
                    trades += 1
                    # Accumulated, not printed: BTCUSDT runs ~25 trades a
                    # second and one line each buries the feature rows.
                    flow.observe(event, book)
                    continue

                # The point of the smoke test: anything but APPLIED means the
                # feed and the book disagree about what a valid sequence is.
                result = book.apply(event)
                if result is not ApplyResult.APPLIED:
                    print(f"!! {result.name} at seq {event.first_seq}-{event.last_seq}")
                    continue

                flow.observe(event, book)

                # Computed once, then both printed and stored -- so what lands
                # in features.parquet is exactly what you watched go past.
                f = feature_snapshot(book, flow, event.timestamp_ns)
                if store is not None:
                    store.write_features(f)

                if f.midprice is None:
                    continue
                mid = f.midprice
                # Scaled to basis points: realised vol over 60s on a liquid
                # pair is ~1e-5, which prints as 0.00 in fixed notation.
                rv_bp = None if f.realised_volatility is None else f.realised_volatility * 10000
                vwap_offset = None if f.vwap is None else f.vwap - mid
                print(
                    f"mid {mid:.2f} "
                    # Offsets, not absolutes: microprice tracks mid to within a
                    # tick and VWAP to within a few, so the lean is the signal.
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
    finally:
        # Closes the Parquet footers. Without them the files hold rows nobody
        # can read, so this has to survive the timeout and a Ctrl-C alike.
        if store is not None:
            store.close()

    print(
        f"\n{events} events, {trades} trade(s), {snapshots} snapshot(s) "
        f"--> more than one snapshot means a resync"
    )
    if store is not None:
        print(f"recorded to {data_root}/binance/{symbol.upper()}/")


def main() -> None:
    symbol = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"
    duration_s = float(sys.argv[2]) if len(sys.argv) > 2 else 30.0
    data_root = sys.argv[3] if len(sys.argv) > 3 else None
    try:
        asyncio.run(watch(symbol, duration_s, data_root))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
