"""The HTTP surface serves live state, and refuses to serve broken state.

Handlers are called directly as coroutines rather than through a client for
most of these: it keeps them synchronous to reason about and needs no server,
no threads and no network. One test drives the real app through `TestClient`
to prove the wiring -- lifespan, background task, routing -- actually holds.
"""

import asyncio
import functools
import time
from decimal import Decimal
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from tickforge import api
from tickforge.api import MarketState
from tickforge.events import BookSnapshot, BookUpdate, Side, Trade

T0 = 1_788_307_200_000_000_000


def async_test(fn):
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


def levels(pairs):
    return tuple((Decimal(p), Decimal(q)) for p, q in pairs)


def snapshot(at=T0, last_seq=100) -> BookSnapshot:
    return BookSnapshot(
        "binance", "BTCUSDT", at, at, last_seq,
        levels((("100", "2"), ("99", "6"))),
        levels((("101", "6"), ("102", "4"))),
    )


def update(seq, at=T0 + 1, bids=(), asks=()) -> BookUpdate:
    return BookUpdate("binance", "BTCUSDT", at, at, seq, seq, levels(bids), levels(asks))


def trade(trade_id=7, price="100.5", side=Side.BUY, at=T0 + 1) -> Trade:
    return Trade("binance", "BTCUSDT", at, at, Decimal(price), Decimal("0.25"), trade_id, side)


def request(state: MarketState):
    """The two attributes the handlers actually reach through."""
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(market=state)))


def synced() -> MarketState:
    """A state with a valid book, one trade and one feature row."""
    state = MarketState("BTCUSDT")
    state.observe(snapshot())
    state.observe(trade())
    state.observe(update(101, bids=(("100", "9"),)))
    return state


# --- serialisation ----------------------------------------------------------


@async_test
async def test_prices_are_strings_not_json_numbers() -> None:
    """The exactness guarantee reaches the wire or it was pointless.

    A JSON number is a float in every client, so 77381.36 would come back as
    77381.36000000000058 -- the failure `decisions.md` chose Decimal to avoid,
    reintroduced at the last possible moment.
    """
    body = await api.get_book(request(synced()), "BTCUSDT", 2)

    assert body["bids"][0] == {"price": "100", "quantity": "9"}
    assert all(isinstance(level["price"], str) for level in body["asks"])


@async_test
async def test_timestamps_stay_integers() -> None:
    """Only prices and quantities are stringified. A nanosecond timestamp is
    exact as an int and a client would have to parse it back."""
    body = await api.get_features(request(synced()), "BTCUSDT")

    assert isinstance(body["timestamp_ns"], int)
    assert isinstance(body["midprice"], str)


@async_test
async def test_undefined_features_stay_null() -> None:
    """None is not zero, and it is not the string 'None' either. A window with
    no price change has no realised volatility, and the wire must say so."""
    body = await api.get_features(request(synced()), "BTCUSDT")

    assert body["realised_volatility"] is None
    assert body["midprice"] == "100.5"


# --- book -------------------------------------------------------------------


@async_test
async def test_book_serves_the_top_of_the_book() -> None:
    body = await api.get_book(request(synced()), "BTCUSDT", 2)

    assert body["state"] == "SYNCED"
    assert body["last_sequence"] == 101
    assert [level["price"] for level in body["bids"]] == ["100", "99"]
    assert [level["price"] for level in body["asks"]] == ["101", "102"]


@async_test
async def test_depth_limits_the_levels_returned() -> None:
    body = await api.get_book(request(synced()), "BTCUSDT", 1)

    assert len(body["bids"]) == 1
    assert len(body["asks"]) == 1


@async_test
async def test_an_invalid_book_is_refused_not_served_stale() -> None:
    """503, not a last-known-good response.

    Serving levels from a book that knows it is wrong is the exact failure the
    book's own reads refuse, and an HTTP layer must not become the way around
    it.
    """
    state = synced()
    state.observe(update(500, bids=(("100", "1"),)))  # a gap
    assert state.book.state.name == "INVALID"

    with pytest.raises(HTTPException) as raised:
        await api.get_book(request(state), "BTCUSDT", 5)

    assert raised.value.status_code == 503


# --- features ---------------------------------------------------------------


@async_test
async def test_features_are_absent_before_the_first_update() -> None:
    """A snapshot alone produces no feature row -- there is no update to
    compute one from yet."""
    state = MarketState("BTCUSDT")
    state.observe(snapshot())

    with pytest.raises(HTTPException) as raised:
        await api.get_features(request(state), "BTCUSDT")

    assert raised.value.status_code == 404


@async_test
async def test_features_are_dropped_when_the_book_breaks() -> None:
    """Otherwise the endpoint keeps serving the last good row indefinitely,
    with nothing in it saying the book underneath is gone."""
    state = synced()
    assert state.latest is not None

    state.observe(update(500, bids=(("100", "1"),)))

    assert state.latest is None
    with pytest.raises(HTTPException):
        await api.get_features(request(state), "BTCUSDT")


@async_test
async def test_a_resync_drops_the_previous_features() -> None:
    """A fresh snapshot means updates were missed, so the last row describes a
    book state that no longer connects to this one."""
    state = synced()
    state.observe(snapshot(at=T0 + 5, last_seq=500))

    assert state.latest is None


# --- trades -----------------------------------------------------------------


@async_test
async def test_trades_are_newest_first() -> None:
    state = MarketState("BTCUSDT")
    state.observe(snapshot())
    for n in range(1, 4):
        state.observe(trade(trade_id=n))

    body = await api.get_trades(request(state), "BTCUSDT", 10)

    assert [row["trade_id"] for row in body["trades"]] == [3, 2, 1]
    assert body["trades"][0]["aggressor"] == "buy"


@async_test
async def test_trade_history_is_bounded() -> None:
    """A deque with maxlen, so a long-running process cannot grow without
    limit. The API holds recent trades, not an archive -- that is storage."""
    state = MarketState("BTCUSDT")
    state.observe(snapshot())
    for n in range(api.TRADE_HISTORY + 50):
        state.observe(trade(trade_id=n))

    assert len(state.trades) == api.TRADE_HISTORY


@async_test
async def test_trades_survive_an_invalid_book() -> None:
    """Each trade is self-contained, so a gap in book updates says nothing
    about them. Only book-derived responses fail closed."""
    state = synced()
    state.observe(update(500, bids=(("100", "1"),)))
    state.observe(trade(trade_id=99, side=Side.SELL))

    body = await api.get_trades(request(state), "BTCUSDT", 10)

    assert body["trades"][0]["trade_id"] == 99


# --- health -----------------------------------------------------------------


@async_test
async def test_health_is_ok_on_a_live_synced_feed() -> None:
    body = await api.get_health(request(synced()))

    assert body["status"] == "ok"
    assert body["stale"] is False
    assert body["events"] == 3
    assert body["snapshots"] == 1
    assert body["error"] is None


@async_test
async def test_health_is_degraded_before_any_data() -> None:
    """No events yet is not healthy. Reporting ok here would mean a feed that
    never connected looks identical to one that is working."""
    body = await api.get_health(request(MarketState("BTCUSDT")))

    assert body["status"] == "degraded"
    assert body["seconds_since_last_event"] is None


@async_test
async def test_health_is_degraded_on_an_invalid_book() -> None:
    state = synced()
    state.observe(update(500, bids=(("100", "1"),)))

    assert (await api.get_health(request(state)))["status"] == "degraded"


@async_test
async def test_health_reports_a_dead_feed_task() -> None:
    """The failure `pitfalls.md` calls silent exception swallowing: without
    this the feed task dies, data stops, and every endpoint keeps answering
    from a frozen book as though nothing happened."""
    state = synced()
    state.error = RuntimeError("socket exploded")

    body = await api.get_health(request(state))

    assert body["status"] == "degraded"
    assert "socket exploded" in body["error"]


@async_test
async def test_health_reports_staleness() -> None:
    """A feed that stopped delivering while the book still looks fine.

    Wall clock, deliberately: staleness is a connection-health question and
    `pitfalls.md` permits real time for exactly that -- it never reaches a
    computed value. The last event is pushed an hour into the past rather than
    the threshold dropped to zero, because `time.time_ns()` resolves to ~600us
    here, so a just-observed event and the health check land in the same tick
    and no positive age elapses at all.
    """
    state = synced()
    state.last_event_ns = time.time_ns() - 3600 * 1_000_000_000

    body = await api.get_health(request(state))

    assert body["stale"] is True
    assert body["status"] == "degraded"
    assert body["seconds_since_last_event"] > api.STALE_AFTER_S


# --- routing ----------------------------------------------------------------


@async_test
async def test_an_untracked_symbol_is_not_served_another_ones_book() -> None:
    """One instance tracks one symbol. Returning BTCUSDT's book for an ETHUSDT
    request would be wrong data that looks entirely correct."""
    for handler in (api.get_book, api.get_features, api.get_trades):
        with pytest.raises(HTTPException) as raised:
            await handler(request(synced()), "ETHUSDT")
        assert raised.value.status_code == 404


@async_test
async def test_symbol_matching_ignores_case() -> None:
    body = await api.get_book(request(synced()), "btcusdt", 1)

    assert body["symbol"] == "BTCUSDT"


# --- wiring -----------------------------------------------------------------


class FakeFeed:
    """Stands in for BinanceFeed: yields a script, then stays open.

    Blocking rather than returning matters -- a feed that ends would exercise
    run_feed's terminal branch instead of the steady state being tested here.
    """

    script: list = []

    def __init__(self, symbol: str) -> None:
        self.symbol = symbol

    async def run(self):
        for event in self.script:
            yield event
        await asyncio.Event().wait()


def test_the_app_serves_state_the_background_task_built(monkeypatch) -> None:
    """The one end-to-end test: lifespan starts the feed, the task fills the
    state, and the routes reach it. Everything above stubs that out."""
    FakeFeed.script = [snapshot(), trade(), update(101, bids=(("100", "9"),))]
    monkeypatch.setattr(api, "BinanceFeed", FakeFeed)

    with TestClient(api.app) as client:
        for _ in range(50):  # let the background task drain the script
            if client.get("/system/health").json()["events"] == 3:
                break
        else:
            pytest.fail("background task never consumed the scripted events")

        book = client.get("/markets/BTCUSDT/book?depth=2")
        assert book.status_code == 200
        assert book.json()["bids"][0]["quantity"] == "9"

        assert client.get("/markets/BTCUSDT/features").status_code == 200
        assert client.get("/markets/BTCUSDT/trades").json()["trades"][0]["trade_id"] == 7
        assert client.get("/markets/ETHUSDT/book").status_code == 404
