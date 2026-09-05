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

from tickforge.adapters.binance_feed import BinanceFeed
from tickforge.analytics import imbalance, market_depth, microprice, midprice
from tickforge.book import ApplyResult, OrderBook
from tickforge.events import BookSnapshot, Side, Trade


async def watch(symbol: str, duration_s: float) -> None:
    book = OrderBook("binance", symbol.upper())
    feed = BinanceFeed(symbol)
    events = 0
    snapshots = 0
    trades = 0
    # Reset on every book line, so each row shows the flow that arrived during
    # that window rather than a running total.
    bought = sold = 0

    try:
        async with asyncio.timeout(duration_s):
            async for event in feed.run():
                events += 1

                if isinstance(event, BookSnapshot):
                    snapshots += 1
                    state = book.load_snapshot(event)
                    print(
                        f"snapshot seq={event.last_seq} "
                        f"{len(event.bids)}x{len(event.asks)} levels -> {state.name}"
                    )
                    continue

                # Must come before the book branch: a trade reaching
                # `book.apply` would be an AttributeError on `last_seq`.
                if isinstance(event, Trade):
                    trades += 1
                    # Counted, not printed: BTCUSDT runs ~25 trades a second
                    # and one line each buries the feature rows entirely.
                    if event.aggressor is Side.BUY:
                        bought += 1
                    else:
                        sold += 1
                    continue

                # The point of the smoke test: anything but APPLIED means the
                # feed and the book disagree about what a valid sequence is.
                result = book.apply(event)
                if result is not ApplyResult.APPLIED:
                    print(f"!! {result.name} at seq {event.first_seq}-{event.last_seq}")
                    continue

                mid = midprice(book)
                if mid is None:
                    continue
                micro = microprice(book)
                bid_depth, ask_depth = market_depth(book, 5)
                print(
                    f"mid {mid:.2f}  "
                    # The offset, not the absolute: microprice tracks mid to
                    # within a tick, so the interesting number is the lean.
                    f"micro {micro - mid:+.4f}  "
                    f"L1 {imbalance(book, 1):+.3f}  "
                    f"L5 {imbalance(book, 5):+.3f}  "
                    f"depth {bid_depth:.2f}/{ask_depth:.2f}  "
                    f"trades {bought}b/{sold}s"
                )
                bought = sold = 0
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
