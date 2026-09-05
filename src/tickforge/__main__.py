"""Run a live Binance feed and print the top of book.

Manual smoke test for the whole ingestion path -- adapter, resync state
machine and order book -- against the real exchange::

    uv run python -m tickforge
    uv run python -m tickforge ETHUSDT 60

Not a test: it needs the network and it prints rather than asserts. Its job is
to surface the failures that only appear against a live venue.
"""

import asyncio
import sys

from tickforge.adapters.binance_feed import BinanceFeed
from tickforge.book import ApplyResult, OrderBook
from tickforge.events import BookSnapshot, Trade


async def watch(symbol: str, duration_s: float) -> None:
    book = OrderBook("binance", symbol.upper())
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
                    print(
                        f"snapshot seq={event.last_seq} "
                        f"{len(event.bids)}x{len(event.asks)} levels -> {state.name}"
                    )
                    continue

                # Must come before the book branch: a trade reaching
                # `book.apply` would be an AttributeError on `last_seq`.
                if isinstance(event, Trade):
                    trades += 1
                    print(
                        f"trade    {event.aggressor.value:<4} "
                        f"{event.quantity} @ {event.price}"
                    )
                    continue

                # The point of the smoke test: anything but APPLIED means the
                # feed and the book disagree about what a valid sequence is.
                result = book.apply(event)
                if result is not ApplyResult.APPLIED:
                    print(f"!! {result.name} at seq {event.first_seq}-{event.last_seq}")
                    continue

                bid, ask = book.best_bid, book.best_ask
                if bid is None or ask is None:
                    continue
                print(f"bid {bid}  ask {ask}  spread {ask - bid}  seq={event.last_seq}")
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
