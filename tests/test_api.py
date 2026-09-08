"""HTTP contracts and feed lifecycle, without connecting to an exchange."""

import asyncio
from dataclasses import replace
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient

from tickforge import api
from tickforge.events import BookSnapshot, BookUpdate, Side, Trade


def snapshot(sequence: int = 100) -> BookSnapshot:
    return BookSnapshot(
        "binance", "BTCUSDT", 1_000_000_000, 1_000_000_000, sequence,
        ((Decimal("77381.36000000"), Decimal("2.00000001")),
         (Decimal("77380"), Decimal("3"))),
        ((Decimal("77382.36"), Decimal("4")),
         (Decimal("77383"), Decimal("5"))),
    )


def update(sequence: int = 101) -> BookUpdate:
    return BookUpdate(
        "binance", "BTCUSDT", 2_000_000_000, 2_000_000_000,
        sequence, sequence, ((Decimal("77381.36000000"), Decimal("3")),), (),
    )


def trade(trade_id: int = 1) -> Trade:
    return Trade(
        "binance", "BTCUSDT", 1_500_000_000, 1_500_000_000,
        Decimal("77381.36000000"), Decimal("0.00000001"), trade_id, Side.SELL,
    )


@pytest.fixture
def client(monkeypatch):
    async def idle_feed(state):
        await asyncio.Event().wait()

    monkeypatch.setattr(api, "run_feed", idle_feed)
    with TestClient(api.app) as client:
        yield client


def observe(client, *events):
    # Use the application's loop, as the real feed does.
    for event in events:
        client.portal.call(api.app.state.market.observe, event)


def test_startup_has_no_book_features_or_trades(client):
    assert client.get("/markets/BTCUSDT/book").status_code == 503
    assert client.get("/markets/BTCUSDT/features").status_code == 404
    assert client.get("/markets/BTCUSDT/trades").json()["trades"] == []
    health = client.get("/system/health").json()
    assert health == {
        "status": "degraded", "symbol": "BTCUSDT", "state": "EMPTY",
        "events": 0, "snapshots": 0, "seconds_since_last_event": None,
        "stale": True, "error": None,
    }


@pytest.mark.parametrize("endpoint", ["book", "features", "trades"])
def test_unknown_symbol_is_not_an_alias(client, endpoint):
    assert client.get(f"/markets/ETHUSDT/{endpoint}").status_code == 404


def test_book_preserves_decimal_strings_and_depth_order(client):
    observe(client, snapshot())
    response = client.get("/markets/btcusdt/book?depth=1")
    assert response.status_code == 200
    assert response.json() == {
        "exchange": "binance", "symbol": "BTCUSDT", "state": "SEEDED",
        "last_sequence": 100,
        "bids": [{"price": "77381.36000000", "quantity": "2.00000001"}],
        "asks": [{"price": "77382.36", "quantity": "4"}],
    }
    full = client.get("/markets/BTCUSDT/book").json()
    assert [row["price"] for row in full["bids"]] == ["77381.36000000", "77380"]
    assert [row["price"] for row in full["asks"]] == ["77382.36", "77383"]


@pytest.mark.parametrize("path", [
    "book?depth=0", "book?depth=-1", "book?depth=5001", "book?depth=abc",
    "trades?limit=0", "trades?limit=-1", "trades?limit=101", "trades?limit=1.5",
])
def test_invalid_query_parameters(client, path):
    assert client.get(f"/markets/BTCUSDT/{path}").status_code == 422


def test_features_include_current_update_and_trade_flow(client):
    observe(client, snapshot(), trade(), update())
    response = client.get("/markets/BTCUSDT/features")
    assert response.status_code == 200
    features = response.json()
    assert features["timestamp_ns"] == 2_000_000_000
    assert features["best_bid"] == "77381.36000000"
    assert features["vwap"] == "77381.36000000"
    assert features["trade_imbalance"] == "-1"
    assert Decimal(features["order_flow_imbalance"]) == Decimal("0.99999999")
    assert features["realised_volatility"] is None
    assert all(isinstance(value, str) or value is None
               for key, value in features.items() if key != "timestamp_ns")


def test_duplicate_updates_do_not_replace_features(client):
    observe(client, snapshot(), update())
    before = client.get("/markets/BTCUSDT/features").json()
    observe(client, replace(update(), timestamp_ns=999_000_000_000))
    assert client.get("/markets/BTCUSDT/features").json() == before
    assert client.get("/system/health").json()["events"] == 3


@pytest.mark.parametrize("bad_event", [
    update(103),
    replace(update(102), bids=((Decimal("80000"), Decimal("1")),)),
    replace(snapshot(200), bids=((Decimal("80000"), Decimal("1")),)),
])
def test_invalid_book_hides_cached_data_and_recovers(client, bad_event):
    observe(client, snapshot(), update(), bad_event, trade())
    assert client.get("/markets/BTCUSDT/book").status_code == 503
    assert client.get("/markets/BTCUSDT/features").status_code == 404
    health = client.get("/system/health").json()
    assert health["state"] == "INVALID"
    assert health["status"] == "degraded"
    assert health["error"] is None
    observe(client, snapshot(300))
    assert client.get("/markets/BTCUSDT/book").status_code == 200
    assert client.get("/markets/BTCUSDT/features").status_code == 404
    observe(client, update(301))
    assert client.get("/markets/BTCUSDT/features").status_code == 200


def test_resnapshot_clears_features_even_without_a_rejected_update(client):
    observe(client, snapshot(), update(), snapshot(200))
    assert client.get("/markets/BTCUSDT/features").status_code == 404


def test_trades_are_bounded_newest_first_and_exact(client):
    observe(client, *(trade(i) for i in range(105)))
    response = client.get("/markets/BTCUSDT/trades?limit=2").json()
    assert [row["trade_id"] for row in response["trades"]] == [104, 103]
    assert response["trades"][0] == {
        "timestamp_ns": 1_500_000_000, "price": "77381.36000000",
        "quantity": "1E-8", "aggressor": "sell", "trade_id": 104,
    }
    history = client.get("/markets/BTCUSDT/trades?limit=100").json()["trades"]
    assert len(history) == 100
    assert history[-1]["trade_id"] == 5
    assert len(client.get("/markets/BTCUSDT/trades").json()["trades"]) == 50


def test_health_tracks_arrival_time_and_terminal_errors(client, monkeypatch):
    now = 100_000_000_000
    monkeypatch.setattr(api.time, "time_ns", lambda: now)
    observe(client, snapshot(), update())
    health = client.get("/system/health").json()
    assert health["status"] == "ok"
    assert health["seconds_since_last_event"] == 0
    assert health["events"] == 2
    assert health["snapshots"] == 1
    now += 31_000_000_000
    health = client.get("/system/health").json()
    assert health["stale"] is True
    assert health["status"] == "degraded"
    # A duplicate refreshes arrival health, but never recomputes features.
    observe(client, update())
    assert client.get("/system/health").json()["status"] == "ok"
    api.app.state.market.error = RuntimeError("feed failed")
    health = client.get("/system/health").json()
    assert health["stale"] is False
    assert health["status"] == "degraded"
    assert health["error"] == "feed failed"


@pytest.mark.parametrize("failure", ["source", "observer", "ended"])
def test_feed_reports_failure_and_closes_source(monkeypatch, failure):
    closed = []

    class FakeFeed:
        def __init__(self, symbol):
            assert symbol == "BTCUSDT"

        async def run(self):
            try:
                yield snapshot()
                if failure == "source":
                    raise RuntimeError("source failed")
            finally:
                closed.append(True)

    monkeypatch.setattr(api, "BinanceFeed", FakeFeed)
    state = api.MarketState("btcusdt")
    if failure == "observer":
        def broken_observe(event):
            raise ValueError("observer failed")

        monkeypatch.setattr(state, "observe", broken_observe)
    asyncio.run(api.run_feed(state))
    expected = "Market feed ended unexpectedly" if failure == "ended" else f"{failure} failed"
    assert str(state.error) == expected
    assert closed == [True]


@pytest.mark.parametrize("body_fails", [False, True])
def test_lifespan_cancels_and_awaits_feed_even_on_error(monkeypatch, body_fails):
    async def scenario():
        started = asyncio.Event()
        closed = asyncio.Event()

        class FakeFeed:
            def __init__(self, symbol):
                pass

            async def run(self):
                try:
                    yield snapshot()
                    started.set()
                    await asyncio.Event().wait()
                finally:
                    closed.set()

        monkeypatch.setattr(api, "BinanceFeed", FakeFeed)
        try:
            async with api.lifespan(api.app):
                await asyncio.wait_for(started.wait(), timeout=1)
                assert api.app.state.market.book.is_valid
                if body_fails:
                    raise ValueError("application failed")
        except ValueError:
            assert body_fails
        assert closed.is_set()
        assert api.app.state.market.error is None

    asyncio.run(scenario())
