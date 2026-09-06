"""Events survive the round trip to Parquet, exactly.

"Exactly" is the whole point. Every assertion on a price here is equality, not
approximate equality -- an approximate check would pass against a float column
and quietly discard the guarantee the project is built on.
"""

import asyncio
import functools
from dataclasses import fields
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from tickforge.analytics import FeatureSnapshot
from tickforge.events import BookSnapshot, BookUpdate, Side, Trade
from tickforge.storage import FEATURE_SCHEMA, EventStore, record

DAY_NS = 24 * 60 * 60 * 1_000_000_000
T0 = 1_788_307_200_000_000_000  # 2026-09-02 00:00:00 UTC
DAY = "2026-09-02"
NEXT_DAY = "2026-09-03"

PRICE = Decimal("77381.36000000")
QTY = Decimal("1.29993000")
LEVELS = ((PRICE, QTY),)


def async_test(fn):
    """Run an async test on a fresh event loop. See test_binance_feed.py."""

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


def snapshot(at=T0, last_seq=7) -> BookSnapshot:
    return BookSnapshot("binance", "BTCUSDT", at, at + 1, last_seq, LEVELS, LEVELS)


def update(at=T0, first_seq=8, last_seq=9) -> BookUpdate:
    return BookUpdate("binance", "BTCUSDT", at, at + 1, first_seq, last_seq, LEVELS, ())


def trade(at=T0, quantity="0.5", aggressor=Side.SELL) -> Trade:
    return Trade("binance", "BTCUSDT", at, at + 1, PRICE, Decimal(quantity), 42, aggressor)


def read(root: Path, stream: str, day: str = DAY) -> list[dict]:
    path = root / "binance" / "BTCUSDT" / day / f"{stream}.parquet"
    return pq.read_table(path).to_pylist()


# --- fidelity ---------------------------------------------------------------


def test_prices_and_quantities_survive_exactly(tmp_path) -> None:
    """Reading back rescales to 18 places, which is numerically identical.

    Safe only because Python hashes Decimals by value: the rescaled price is
    the same order-book dict key. Were it otherwise, every replayed level would
    land on a fresh key and the book would silently double -- so this asserts
    the hash too, not just equality.
    """
    with EventStore(tmp_path, "binance", "BTCUSDT") as store:
        store.write(update())

    level = read(tmp_path, "book_updates")[0]["bids"][0]
    assert level["price"] == PRICE
    assert level["quantity"] == QTY
    assert isinstance(level["price"], Decimal)
    assert hash(level["price"]) == hash(PRICE)


def test_a_trade_round_trips_whole(tmp_path) -> None:
    """`aggressor` needs no conversion in either direction -- a StrEnum member
    is its string. That is what the type was chosen for."""
    with EventStore(tmp_path, "binance", "BTCUSDT") as store:
        store.write(trade())

    row = read(tmp_path, "trades")[0]
    assert row["exchange"] == "binance"
    assert row["symbol"] == "BTCUSDT"
    assert row["trade_id"] == 42
    assert row["aggressor"] == "sell"
    assert Side(row["aggressor"]) is Side.SELL
    assert row["price"] == PRICE


def test_each_event_type_lands_in_its_own_file(tmp_path) -> None:
    with EventStore(tmp_path, "binance", "BTCUSDT") as store:
        store.write(snapshot())
        store.write(update())
        store.write(trade())

    assert len(read(tmp_path, "snapshots")) == 1
    assert len(read(tmp_path, "book_updates")) == 1
    assert len(read(tmp_path, "trades")) == 1


def test_an_unused_stream_leaves_no_file(tmp_path) -> None:
    """An empty trades.parquet would read as 'nothing traded' rather than
    'nothing was recorded'. Writers open lazily so the file never appears."""
    with EventStore(tmp_path, "binance", "BTCUSDT") as store:
        store.write(update())

    directory = tmp_path / "binance" / "BTCUSDT" / DAY
    assert not (directory / "trades.parquet").exists()


def test_sequence_range_is_preserved(tmp_path) -> None:
    """Replay needs the update boundary intact -- the range describes the whole
    batch of levels, so a schema that lost it could not reconstruct the book."""
    with EventStore(tmp_path, "binance", "BTCUSDT") as store:
        store.write(update(first_seq=101, last_seq=110))

    row = read(tmp_path, "book_updates")[0]
    assert (row["first_seq"], row["last_seq"]) == (101, 110)


def test_an_event_for_another_instrument_is_refused(tmp_path) -> None:
    """The row's identity columns come from the store, not the event.

    So a mismatched event would be silently relabelled and filed under the
    wrong symbol -- permanently, and looking entirely correct on read. Loud is
    the only acceptable behaviour.
    """
    ether = BookUpdate("binance", "ETHUSDT", T0, T0, 1, 1, LEVELS, ())

    with EventStore(tmp_path, "binance", "BTCUSDT") as store:
        with pytest.raises(ValueError, match="ETHUSDT"):
            store.write(ether)


def test_an_event_from_another_venue_is_refused(tmp_path) -> None:
    okx = BookUpdate("okx", "BTCUSDT", T0, T0, 1, 1, LEVELS, ())

    with EventStore(tmp_path, "binance", "BTCUSDT") as store:
        with pytest.raises(ValueError, match="okx"):
            store.write(okx)


def test_an_unknown_event_type_is_refused(tmp_path) -> None:
    """A new event type must not land in book_updates by fallthrough and die
    on a missing field, which would say nothing about the real cause."""

    class MarketStatus:
        exchange, symbol = "binance", "BTCUSDT"
        timestamp_ns = received_ns = T0

    with EventStore(tmp_path, "binance", "BTCUSDT") as store:
        with pytest.raises(ValueError, match="MarketStatus"):
            store.write(MarketStatus())


# --- partitioning -----------------------------------------------------------


def test_a_new_utc_day_opens_a_new_partition(tmp_path) -> None:
    with EventStore(tmp_path, "binance", "BTCUSDT") as store:
        store.write(update(at=T0))
        store.write(update(at=T0 + DAY_NS))

    assert len(read(tmp_path, "book_updates", DAY)) == 1
    assert len(read(tmp_path, "book_updates", NEXT_DAY)) == 1


def test_the_old_partition_is_closed_when_the_day_rolls(tmp_path) -> None:
    """Readable before the store is closed, because rolling closed its footer.
    A long capture would otherwise leave every past day unreadable until the
    process exited."""
    store = EventStore(tmp_path, "binance", "BTCUSDT")
    store.write(update(at=T0))
    store.write(update(at=T0 + DAY_NS))

    assert len(read(tmp_path, "book_updates", DAY)) == 1  # no close() yet
    store.close()


def test_the_partition_date_comes_from_event_time(tmp_path) -> None:
    """Not the wall clock. Replaying a 2026 capture in 2027 has to write into
    the 2026 partition, or a replay silently rewrites history under today."""
    with EventStore(tmp_path, "binance", "BTCUSDT") as store:
        store.write(update(at=T0))

    assert (tmp_path / "binance" / "BTCUSDT" / DAY).is_dir()


# --- batching ---------------------------------------------------------------


def test_flush_happens_on_row_count(tmp_path) -> None:
    """Asserted on the buffer rather than the file: a flush writes a row group
    but not a footer, so the file stays unreadable until close."""
    store = EventStore(tmp_path, "binance", "BTCUSDT", batch_rows=3)
    store.write(update())
    store.write(update())
    assert len(store._buffers["book_updates"]) == 2

    store.write(update())
    assert store._buffers["book_updates"] == []
    store.close()


def test_flush_happens_on_elapsed_event_time(tmp_path) -> None:
    """The second bound, and the one that matters on a quiet symbol: without
    it a half-full buffer sits unwritten for hours."""
    store = EventStore(tmp_path, "binance", "BTCUSDT", batch_rows=10_000, batch_ns=60)
    store.write(update(at=T0))
    assert len(store._buffers["book_updates"]) == 1

    store.write(update(at=T0 + 60))
    assert store._buffers["book_updates"] == []
    store.close()


def test_buffered_rows_are_not_lost_on_close(tmp_path) -> None:
    """A partial batch at shutdown still has to reach the file."""
    store = EventStore(tmp_path, "binance", "BTCUSDT", batch_rows=10_000)
    store.write(update())
    store.close()

    assert len(read(tmp_path, "book_updates")) == 1


# --- record -----------------------------------------------------------------


@async_test
async def test_record_yields_every_event_it_writes(tmp_path) -> None:
    """The tee is transparent: what the consumer sees is what the file gets.

    That equivalence is the Phase 6 guarantee. An explicit store.write() in
    each consumer would make it a convention instead, and a recorded file
    cannot show whether a caller honoured one.
    """
    events = [snapshot(), update(), trade()]

    async def source():
        for event in events:
            yield event

    with EventStore(tmp_path, "binance", "BTCUSDT") as store:
        seen = [event async for event in record(source(), store)]

    assert seen == events
    assert len(read(tmp_path, "snapshots")) == 1
    assert len(read(tmp_path, "book_updates")) == 1
    assert len(read(tmp_path, "trades")) == 1


@async_test
async def test_record_preserves_order(tmp_path) -> None:
    async def source():
        for seq in range(10):
            yield update(first_seq=seq, last_seq=seq)

    with EventStore(tmp_path, "binance", "BTCUSDT") as store:
        async for _ in record(source(), store):
            pass

    assert [row["first_seq"] for row in read(tmp_path, "book_updates")] == list(range(10))


# --- the footer -------------------------------------------------------------


# --- features ---------------------------------------------------------------


def features(at=T0, **overrides) -> FeatureSnapshot:
    """A snapshot with everything populated, so overrides can blank one field."""
    defaults = dict(
        timestamp_ns=at,
        best_bid=Decimal("100"),
        best_ask=Decimal("101"),
        spread=Decimal("1"),
        midprice=Decimal("100.5"),
        microprice=Decimal("100.25"),
        imbalance_1=Decimal("-0.5"),
        imbalance_5=Decimal("-0.2"),
        imbalance_10=Decimal("0"),
        bid_depth_10=Decimal("16"),
        ask_depth_10=Decimal("16"),
        vwap=Decimal("100.4"),
        trade_imbalance=Decimal("0.5"),
        order_flow_imbalance=Decimal("4"),
        realised_volatility=Decimal("0.00248"),
    )
    return FeatureSnapshot(**{**defaults, **overrides})


def test_the_schema_matches_the_dataclass(tmp_path) -> None:
    """A new feature must reach storage by being added in one place.

    Without this, adding a field to FeatureSnapshot silently drops it: the
    extra key is ignored against an explicit Arrow schema, so nothing fails
    and the column simply never appears.
    """
    from_schema = set(FEATURE_SCHEMA.names) - {"exchange", "symbol"}
    assert from_schema == {field.name for field in fields(FeatureSnapshot)}


def test_features_round_trip(tmp_path) -> None:
    with EventStore(tmp_path, "binance", "BTCUSDT") as store:
        store.write_features(features())

    row = read(tmp_path, "features")[0]
    assert row["midprice"] == Decimal("100.5")
    assert row["imbalance_1"] == Decimal("-0.5")
    assert row["order_flow_imbalance"] == Decimal("4")
    assert row["timestamp_ns"] == T0


def test_a_missing_feature_stays_null_rather_than_zero(tmp_path) -> None:
    """The distinction the nullable columns exist for.

    An empty window has no VWAP and a resync leaves no order flow. Writing
    those as zero would read downstream as "traded at zero" and "perfectly
    balanced" -- both plausible, both wrong.
    """
    with EventStore(tmp_path, "binance", "BTCUSDT") as store:
        store.write_features(features(vwap=None, order_flow_imbalance=None))

    row = read(tmp_path, "features")[0]
    assert row["vwap"] is None
    assert row["order_flow_imbalance"] is None
    assert row["midprice"] == Decimal("100.5")  # its neighbours are unaffected


def test_a_division_result_is_rounded_rather_than_rejected(tmp_path) -> None:
    """Dividing Decimals gives 28 significant digits -- 29 decimal places here
    -- and pyarrow refuses to rescale rather than truncating silently. So the
    rounding happens explicitly on the way in.
    """
    ratio = (Decimal(12) - Decimal(14)) / (Decimal(12) + Decimal(14))
    assert -ratio.as_tuple().exponent == 29  # too many places for the column

    with EventStore(tmp_path, "binance", "BTCUSDT") as store:
        store.write_features(features(imbalance_5=ratio))

    stored = read(tmp_path, "features")[0]["imbalance_5"]
    assert stored == ratio.quantize(Decimal("1e-18"))
    assert abs(stored - ratio) < Decimal("1e-18")


def test_features_are_a_separate_stream(tmp_path) -> None:
    """Derived rows never mix with the raw stream replay reproduces."""
    with EventStore(tmp_path, "binance", "BTCUSDT") as store:
        store.write(update())
        store.write_features(features())

    assert len(read(tmp_path, "features")) == 1
    assert len(read(tmp_path, "book_updates")) == 1


def test_features_roll_partitions_on_event_time(tmp_path) -> None:
    with EventStore(tmp_path, "binance", "BTCUSDT") as store:
        store.write_features(features(at=T0))
        store.write_features(features(at=T0 + DAY_NS))

    assert len(read(tmp_path, "features", DAY)) == 1
    assert len(read(tmp_path, "features", NEXT_DAY)) == 1


# --- the footer -------------------------------------------------------------


def test_a_file_is_unreadable_until_the_store_is_closed(tmp_path) -> None:
    """Pins the reason close() is a correctness requirement, not politeness.

    Parquet keeps its schema and row-group index in a footer written at close.
    Without it there is no way to find anything in the file.
    """
    store = EventStore(tmp_path, "binance", "BTCUSDT", batch_rows=1)
    store.write(update())  # flushed a row group; no footer yet

    with pytest.raises(Exception):
        read(tmp_path, "book_updates")

    store.close()
    assert len(read(tmp_path, "book_updates")) == 1
