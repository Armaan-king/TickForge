"""The research boundary serves recorded data in replay order, and only that.

Two captures are written into one day on purpose. Each `EventStore` starts its
`capture_seq` at zero, so a day holding two of them is the case that breaks any
ordering built on `capture_seq` alone -- and it is the reason the cursor
carries `(session, capture_seq)`.
"""

import asyncio
import io
from decimal import Decimal

import polars as pl
import pytest
from fastapi.testclient import TestClient

from tickforge import api, research
from tickforge.events import BookSnapshot, BookUpdate, Side, Trade
from tickforge.replay import read_partition
from tickforge.storage import EventStore

T0 = 1_788_307_200_000_000_000  # 2026-09-02 00:00:00 UTC
DAY = "2026-09-02"

PRICE = Decimal("77381.36000000")
QTY = Decimal("1.29993000")
LEVELS = ((PRICE, QTY), (Decimal("77380"), Decimal("0")))


def snapshot(at=T0, last_seq=100) -> BookSnapshot:
    return BookSnapshot("binance", "BTCUSDT", at, at + 1, last_seq, LEVELS, LEVELS)


def update(seq, at=T0) -> BookUpdate:
    return BookUpdate("binance", "BTCUSDT", at, at + 1, seq, seq + 2, LEVELS, ())


def trade(trade_id, at=T0, side=Side.BUY) -> Trade:
    return Trade("binance", "BTCUSDT", at, at + 1, PRICE, QTY, trade_id, side)


def session_events(base: int, n: int) -> list:
    """A snapshot then an alternating stream, so every type is represented."""
    events = [snapshot(at=base)]
    for i in range(n):
        at = base + (i + 1) * 1_000_000
        events.append(
            trade(base + i, at=at, side=Side.BUY if i % 2 else Side.SELL)
            if i % 3
            else update(200 + i, at=at)
        )
    return events


@pytest.fixture
def capture(tmp_path, monkeypatch):
    """Two separate captures on one UTC day, both starting capture_seq at 0."""
    first = session_events(T0, 30)
    second = session_events(T0 + 60_000_000_000, 20)

    for stamp, events in ((1, first), (2, second)):
        with EventStore(tmp_path, "binance", "BTCUSDT", session=stamp) as store:
            for event in events:
                store.write(event)

    monkeypatch.setattr(research, "data_root", lambda: tmp_path)
    return tmp_path, first + second


@pytest.fixture
def client(monkeypatch, capture):
    """The real app, with the live feed replaced by one that never yields."""

    async def idle(state):
        await asyncio.Event().wait()

    monkeypatch.setattr(api, "run_feed", idle)
    with TestClient(api.app) as started:
        yield started


# --- ordering ---------------------------------------------------------------


def test_order_is_identical_to_replay(capture) -> None:
    """The guarantee the whole boundary rests on.

    If these ever diverge, a model trains on one ordering while the pipeline
    that produced the data used another, and nothing anywhere reports it.
    """
    root, _ = capture
    pairs, _ = research.read_range(root, "BTCUSDT", DAY, limit=10_000)

    assert [event for _, event in pairs] == read_partition(root, "binance", "BTCUSDT", DAY)


def test_capture_seq_alone_cannot_order_two_captures(capture) -> None:
    """Both captures number from zero, so ordering on capture_seq would
    interleave them. Sessions must sort first."""
    root, _ = capture
    pairs, _ = research.read_range(root, "BTCUSDT", DAY, limit=10_000)

    sessions = [stamp for stamp, _ in pairs]
    assert sessions == sorted(sessions)
    assert set(sessions) == {1, 2}
    # The first event of session 2 shares a capture_seq with one in session 1.
    assert sessions.index(2) > 0


# --- filtering --------------------------------------------------------------


def test_filter_by_session(capture) -> None:
    root, _ = capture
    pairs, _ = research.read_range(root, "BTCUSDT", DAY, session=2, limit=10_000)

    assert pairs
    assert {stamp for stamp, _ in pairs} == {2}


def test_filter_by_capture_seq_range(capture) -> None:
    root, _ = capture
    pairs, _ = research.read_range(
        root, "BTCUSDT", DAY, session=1, start_seq=5, end_seq=9, limit=10_000
    )

    assert len(pairs) == 5


def test_filter_by_timestamp_range(capture) -> None:
    """Filtering on time is a convenience; ordering never uses it, because
    both timestamp columns have ties and disagree with each other."""
    root, _ = capture
    pairs, _ = research.read_range(
        root, "BTCUSDT", DAY, start_ns=T0 + 5_000_000, end_ns=T0 + 9_000_000, limit=10_000
    )

    assert pairs
    assert all(T0 + 5_000_000 <= e.timestamp_ns <= T0 + 9_000_000 for _, e in pairs)


def test_a_missing_partition_is_a_404(client) -> None:
    assert client.get("/research/BTCUSDT/events", params={"date": "1999-01-01"}).status_code == 404


def test_an_empty_range_is_not_an_error(client) -> None:
    """No data in a window is a fact, not a failure. A consumer walking a day
    hour by hour will hit quiet windows and must not have to treat them as
    errors."""
    body = client.get(
        "/research/BTCUSDT/events",
        params={"date": DAY, "start_seq": 900_000, "end_seq": 900_100},
    ).json()

    assert body["count"] == 0
    assert body["events"] == []
    assert body["next_cursor"] is None


# --- pagination -------------------------------------------------------------


def test_a_cursor_walk_covers_every_event_exactly_once(client, capture) -> None:
    """Gaps lose training data; duplicates corrupt it. Both are silent."""
    _, written = capture

    seen, cursor = [], None
    for _ in range(50):
        params = {"date": DAY, "limit": 7} | ({"cursor": cursor} if cursor else {})
        page = client.get("/research/BTCUSDT/events", params=params).json()
        seen.extend(page["events"])
        cursor = page["next_cursor"]
        if cursor is None:
            break

    assert cursor is None, "walk did not terminate"
    assert len(seen) == len(written)
    # Timestamps are unique across the fixture, so comparing them in order is a
    # full identity check: it catches a dropped event, a repeated one, and any
    # reordering, which counting alone would not.
    stamps = [e["timestamp_ns"] for e in seen]
    assert len(set(stamps)) == len(stamps), "an event was served twice"
    assert stamps == [e.timestamp_ns for e in written]


def test_the_byte_budget_ends_a_page_early(capture) -> None:
    """A row limit alone cannot bound a response: a trade is ~264 bytes of
    JSON and a 2,000-level snapshot is 150 KB."""
    root, _ = capture
    pairs, cursor = research.read_range(root, "BTCUSDT", DAY, limit=10_000, byte_budget=1)

    assert len(pairs) == 1  # the budget is checked before appending the next
    assert cursor is not None


def test_a_malformed_cursor_is_rejected(client) -> None:
    assert client.get(
        "/research/BTCUSDT/events", params={"date": DAY, "cursor": "not-a-cursor"}
    ).status_code == 400


def test_limit_is_capped(client) -> None:
    assert client.get(
        "/research/BTCUSDT/events", params={"date": DAY, "limit": research.MAX_LIMIT + 1}
    ).status_code == 422


# --- field fidelity ---------------------------------------------------------


def test_every_documented_field_is_exposed(client) -> None:
    body = client.get("/research/BTCUSDT/events", params={"date": DAY, "limit": 60}).json()
    by_type = {e["type"]: e for e in body["events"]}

    common = {"type", "session", "exchange", "symbol", "timestamp_ns", "received_ns"}
    assert common <= set(by_type["snapshot"])
    assert common <= set(by_type["trade"])
    assert common <= set(by_type["book_update"])

    assert {"last_seq", "bids", "asks"} <= set(by_type["snapshot"])
    assert {"first_seq", "last_seq", "bids", "asks"} <= set(by_type["book_update"])
    assert {"trade_id", "price", "quantity", "aggressor"} <= set(by_type["trade"])


def test_decimals_cross_the_wire_as_exact_strings(client) -> None:
    """A JSON number is a float in every client, and 77381.36 comes back wrong
    in the twelfth decimal -- discarding the exactness the pipeline preserves
    at the very last step."""
    body = client.get("/research/BTCUSDT/events", params={"date": DAY, "limit": 60}).json()
    trade_row = next(e for e in body["events"] if e["type"] == "trade")

    assert isinstance(trade_row["price"], str)
    assert Decimal(trade_row["price"]) == PRICE
    assert Decimal(trade_row["quantity"]) == QTY


def test_zero_quantity_levels_are_preserved(client) -> None:
    """A quantity of zero is a deletion. Dropping it -- or relabelling it as a
    CANCEL -- would be inferring intent, which belongs to the consumer."""
    body = client.get("/research/BTCUSDT/events", params={"date": DAY, "limit": 60}).json()
    update_row = next(e for e in body["events"] if e["type"] == "book_update")

    assert any(Decimal(level["quantity"]) == 0 for level in update_row["bids"])


def test_no_inferred_event_types_are_emitted(client) -> None:
    """TickForge reports market facts. CANCEL and EXECUTE are modelling."""
    body = client.get("/research/BTCUSDT/events", params={"date": DAY, "limit": 60}).json()

    assert {e["type"] for e in body["events"]} <= {"snapshot", "book_update", "trade"}


# --- catalogue --------------------------------------------------------------


def test_sessions_reports_both_captures(client) -> None:
    body = client.get("/research/BTCUSDT/sessions").json()

    assert [d["date"] for d in body["dates"]] == [DAY]
    day = body["dates"][0]
    assert [s["session"] for s in day["sessions"]] == [1, 2]
    assert day["events"] == body["events"]


def test_sessions_reports_ranges_without_reading_data(client) -> None:
    """Row-group statistics from the footer, so cataloguing a large partition
    stays cheap enough to call on every request."""
    day = client.get("/research/BTCUSDT/sessions").json()["dates"][0]
    trades = day["sessions"][0]["streams"]["trades"]

    assert trades["rows"] > 0
    low, high = trades["capture_seq"]
    assert 0 <= low <= high


def test_an_unknown_symbol_lists_nothing(client) -> None:
    body = client.get("/research/DOGEUSDT/sessions").json()

    assert body["dates"] == []
    assert body["events"] == 0


def test_features_are_not_served(client) -> None:
    """Derived output. A consumer recomputes features from the events, which
    is what keeps them reproducible."""
    day = client.get("/research/BTCUSDT/sessions").json()["dates"][0]

    assert all("features" not in s["streams"] for s in day["sessions"])
    assert client.get(
        "/research/BTCUSDT/export", params={"date": DAY, "stream": "features"}
    ).status_code == 400


# --- bulk export ------------------------------------------------------------


def test_export_returns_one_readable_parquet(client, capture) -> None:
    root, _ = capture
    response = client.get("/research/BTCUSDT/export", params={"date": DAY, "stream": "trades"})

    assert response.status_code == 200
    assert "parquet" in response.headers["content-type"]
    frame = pl.read_parquet(io.BytesIO(response.content))

    expected = sum(1 for _, e in research.read_range(root, "BTCUSDT", DAY, limit=10_000)[0]
                   if isinstance(e, Trade))
    assert frame.height == expected


def test_export_preserves_the_decimal_type(client) -> None:
    """The stored schema is decimal128(38,18); a download that widened it to a
    float would be unrecoverable for the consumer."""
    response = client.get("/research/BTCUSDT/export", params={"date": DAY, "stream": "trades"})
    frame = pl.read_parquet(io.BytesIO(response.content))

    assert frame.schema["price"] == pl.Decimal(precision=38, scale=18)
    assert frame["price"][0] == PRICE


def test_export_carries_every_column_replay_needs(client) -> None:
    response = client.get("/research/BTCUSDT/export", params={"date": DAY, "stream": "book_updates"})
    frame = pl.read_parquet(io.BytesIO(response.content))

    assert {"exchange", "symbol", "timestamp_ns", "received_ns", "session",
            "capture_seq", "first_seq", "last_seq", "bids", "asks"} <= set(frame.columns)


def test_export_merges_sessions_in_order(client) -> None:
    """One request spans every session of the day, ordered, so a consumer does
    not fetch one file per roll."""
    response = client.get("/research/BTCUSDT/export", params={"date": DAY, "stream": "trades"})
    frame = pl.read_parquet(io.BytesIO(response.content))

    assert frame["session"].to_list() == sorted(frame["session"].to_list())
    for stamp in frame["session"].unique():
        block = frame.filter(pl.col("session") == stamp)["capture_seq"].to_list()
        assert block == sorted(block)


def test_export_carries_the_session_column(client) -> None:
    """The stored files hold the session in their filename; a merged download
    has no filename per row.

    Without this column a consumer sorting by capture_seq -- the obvious thing
    to do with a column named that -- silently interleaves two captures, since
    each numbers from zero. The fixture writes two captures precisely so this
    is reachable.
    """
    response = client.get("/research/BTCUSDT/export", params={"date": DAY, "stream": "trades"})
    frame = pl.read_parquet(io.BytesIO(response.content))

    assert "session" in frame.columns
    assert set(frame["session"].unique()) == {1, 2}
    # Both captures reuse low capture_seq values, so sorting on it alone is
    # genuinely ambiguous here rather than only in theory.
    overlap = set(frame.filter(pl.col("session") == 1)["capture_seq"]) & set(
        frame.filter(pl.col("session") == 2)["capture_seq"]
    )
    assert overlap, "fixture no longer exercises the ambiguity this column fixes"


def test_export_can_be_scoped_to_one_session(client) -> None:
    whole = pl.read_parquet(io.BytesIO(
        client.get("/research/BTCUSDT/export", params={"date": DAY, "stream": "trades"}).content))
    one = pl.read_parquet(io.BytesIO(
        client.get("/research/BTCUSDT/export",
                   params={"date": DAY, "stream": "trades", "session": 1}).content))

    assert 0 < one.height < whole.height


def test_export_of_a_missing_partition_is_a_404(client) -> None:
    assert client.get(
        "/research/BTCUSDT/export", params={"date": "1999-01-01", "stream": "trades"}
    ).status_code == 404


def test_export_rejects_an_unknown_stream(client) -> None:
    assert client.get(
        "/research/BTCUSDT/export", params={"date": DAY, "stream": "orders"}
    ).status_code == 400


# --- no regressions ---------------------------------------------------------


def test_live_endpoints_still_answer(client) -> None:
    """The research routes touch storage only, so they must not have disturbed
    the live surface -- nor depend on it being healthy."""
    assert client.get("/system/health").json()["symbol"] == "BTCUSDT"
    assert client.get("/markets/BTCUSDT/book").status_code == 503  # no feed in tests
    assert client.get("/markets/ETHUSDT/trades").status_code == 404


def test_research_answers_while_the_feed_is_down(client) -> None:
    """A degraded feed must not take historical access with it."""
    assert client.get("/system/health").json()["status"] == "degraded"
    assert client.get("/research/BTCUSDT/sessions").status_code == 200
