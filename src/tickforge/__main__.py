"""Run a live Binance feed and print microstructure features.

Manual smoke test for the whole pipeline -- adapter, resync state machine,
order book and analytics -- against the real exchange::

    uv run python -m tickforge
    uv run python -m tickforge ETHUSDT 60

Not a test: it needs the network and it prints rather than asserts. Its job is
to surface what only appears against a live venue -- whether microprice
actually leads price, whether imbalance moves before the touch does, and
whether any of it survives a real resync.
"""

import asyncio
import sys

from decimal import Decimal

from tickforge.adapters.binance_feed import BinanceFeed
from tickforge.analytics import FlowFeatures, imbalance, microprice, midprice
from tickforge.book import ApplyResult, OrderBook
from tickforge.events import BookSnapshot, Trade

WINDOW_NS = 60 * 1_000_000_000


def show(value: Decimal | None, spec: str) -> str:
    """Format a feature, or a dash where it has nothing to report yet."""
    return "--".rjust(len(format(Decimal(0), spec))) if value is None else format(value, spec)


async def watch(symbol: str, duration_s: float) -> None:
    book = OrderBook("binance", symbol.upper())
    flow = FlowFeatures(WINDOW_NS)
    feed = BinanceFeed(symbol)
    events = 0
    snapshots = 0
    trades = 0

    try:
        async with asyncio.timeout(duration_s):
            async for event in feed.run():
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

                mid = midprice(book)
                if mid is None:
                    continue
                vwap = flow.vwap
                rvol = flow.realised_volatility
                print(
                    f"mid {mid:.2f} "
                    # Offsets, not absolutes: microprice tracks mid to within a
                    # tick and VWAP to within a few, so the lean is the signal.
                    f"micro {microprice(book) - mid:+.4f} | "
                    f"L1 {imbalance(book, 1):+.2f} "
                    f"L5 {imbalance(book, 5):+.2f} | "
                    f"ofi {flow.order_flow_imbalance:+8.3f} "
                    f"tImb {show(flow.trade_imbalance, '+.2f')} "
                    f"vwap {show(None if vwap is None else vwap - mid, '+8.2f')} "
                    # Scaled to basis points: realised vol over 60s on a liquid
                    # pair is ~1e-5, which prints as 0.00 in fixed notation.
                    f"rv {show(None if rvol is None else rvol * 10000, '.2f')}bp"
                )
    except TimeoutError:
        pass

    print(
        f"\n{events} events, {trades} trade(s), {snapshots} snapshot(s) "
        f"--> more than one snapshot means a resync"
    )


def main() -> None:
    symbol = sys.argv[1] if len(sys.argv) > 1 else "BTCUSDT"
    duration_s = float(sys.argv[2]) if len(sys.argv) > 2 else 30.0
    try:
        asyncio.run(watch(symbol, duration_s))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
