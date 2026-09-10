"""HTTP interface over live market state.

Phase 10. Runs the feed itself in a background task and serves the state it
maintains, so consumers get book, features and trades without touching an
exchange WebSocket.

Prices and quantities go out as strings so clients can preserve their exact
decimal values instead of parsing them as binary floats.

Run with ``uv run uvicorn tickforge.api:app``. One instance tracks BTCUSDT.
"""

import asyncio
import time
from collections import deque
from collections.abc import AsyncGenerator
from contextlib import aclosing, asynccontextmanager, suppress
from dataclasses import asdict
from decimal import Decimal
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query, Request

from tickforge import research
from tickforge.adapters.binance_feed import BinanceFeed
from tickforge.analytics import FeatureSnapshot, FlowFeatures, feature_snapshot
from tickforge.book import ApplyResult, OrderBook
from tickforge.events import BookSnapshot, MarketEvent, Trade

WINDOW_NS = 60 * 1_000_000_000
TRADE_HISTORY = 100
STALE_AFTER_S = 30.0


def _price(value: Decimal | None) -> str | None:
    """Keep exact decimal values and undefined values distinct on the wire."""
    return None if value is None else str(value)


class MarketState:
    """State written by the feed task and read by async endpoint handlers.

    Observe never awaits, so each event is processed atomically on the loop.
    Health uses arrival time; analytics continue to use event time only.
    """

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol.upper()
        self.book = OrderBook("binance", self.symbol)
        self.flow = FlowFeatures(WINDOW_NS)
        self.latest: FeatureSnapshot | None = None
        self.trades: deque[Trade] = deque(maxlen=TRADE_HISTORY)
        self.events = 0
        self.snapshots = 0
        self.last_event_ns = 0
        self.error: Exception | None = None

    def observe(self, event: MarketEvent) -> None:
        """Dispatch normalized events through the existing book and analytics."""
        self.events += 1
        self.last_event_ns = time.time_ns()

        if isinstance(event, BookSnapshot):
            self.snapshots += 1
            self.latest = None
            self.book.load_snapshot(event)
            if self.book.is_valid:
                self.flow.observe(event, self.book)
        elif isinstance(event, Trade):
            self.trades.append(event)
            self.flow.observe(event, self.book)
        else:
            result = self.book.apply(event)
            if result is ApplyResult.APPLIED:
                self.flow.observe(event, self.book)
                self.latest = feature_snapshot(self.book, self.flow, event.timestamp_ns)
            elif not self.book.is_valid:
                self.latest = None


async def run_feed(state: MarketState) -> None:
    """Expose terminal failures and close the feed even if observation fails."""
    try:
        async with aclosing(BinanceFeed(state.symbol).run()) as source:
            async for event in source:
                state.observe(event)
        state.error = RuntimeError("Market feed ended unexpectedly")
    except Exception as error:
        # CancelledError deliberately propagates to lifespan on shutdown.
        state.error = error


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Own the feed task for exactly the application's lifetime."""
    state = MarketState("BTCUSDT")
    app.state.market = state
    task = asyncio.create_task(run_feed(state))
    try:
        yield
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


app = FastAPI(lifespan=lifespan, title="TickForge")

# Read-only access to recorded captures. Deliberately independent of the live
# feed above: these routes touch storage only, so they keep answering when the
# feed is degraded or was never started.
app.include_router(research.router)


def _state_for(request: Request, symbol: str) -> MarketState:
    state: MarketState = request.app.state.market
    if symbol.upper() != state.symbol:
        raise HTTPException(status_code=404, detail="Symbol is not tracked")
    return state


# Async handlers stay on the writer's event loop. Sync handlers would run in
# a thread pool, invalidating the no-lock assumption even without any awaits.
@app.get("/markets/{symbol}/book")
async def get_book(
    request: Request,
    symbol: str,
    depth: Annotated[int, Query(ge=1, le=5000)] = 10,
) -> dict:
    state = _state_for(request, symbol)
    book = state.book
    if not book.is_valid:
        raise HTTPException(status_code=503, detail="Book is not valid; awaiting snapshot")
    return {
        "exchange": book.exchange,
        "symbol": state.symbol,
        "state": book.state.name,
        "last_sequence": book.last_sequence,
        "bids": [
            {"price": _price(price), "quantity": _price(quantity)}
            for price, quantity in book.top_bids(depth)
        ],
        "asks": [
            {"price": _price(price), "quantity": _price(quantity)}
            for price, quantity in book.top_asks(depth)
        ],
    }


@app.get("/markets/{symbol}/features")
async def get_features(request: Request, symbol: str) -> dict:
    state = _state_for(request, symbol)
    if state.latest is None:
        raise HTTPException(status_code=404, detail="No features available; awaiting book update")
    return {
        key: _price(value) if isinstance(value, Decimal) or value is None else value
        for key, value in asdict(state.latest).items()
    }


@app.get("/markets/{symbol}/trades")
async def get_trades(
    request: Request,
    symbol: str,
    limit: Annotated[int, Query(ge=1, le=TRADE_HISTORY)] = 50,
) -> dict:
    state = _state_for(request, symbol)
    return {
        "exchange": state.book.exchange,
        "symbol": state.symbol,
        "trades": [
            {
                "timestamp_ns": trade.timestamp_ns,
                "price": _price(trade.price),
                "quantity": _price(trade.quantity),
                "aggressor": trade.aggressor.value,
                "trade_id": trade.trade_id,
            }
            for trade in list(reversed(state.trades))[:limit]
        ],
    }


@app.get("/system/health")
async def get_health(request: Request) -> dict:
    state: MarketState = request.app.state.market
    age = (
        max(0.0, (time.time_ns() - state.last_event_ns) / 1_000_000_000)
        if state.last_event_ns else None
    )
    stale = age is None or age > STALE_AFTER_S
    healthy = state.book.is_valid and not stale and state.error is None
    return {
        "status": "ok" if healthy else "degraded",
        "symbol": state.symbol,
        "state": state.book.state.name,
        "events": state.events,
        "snapshots": state.snapshots,
        "seconds_since_last_event": age,
        "stale": stale,
        "error": None if state.error is None else str(state.error),
    }
