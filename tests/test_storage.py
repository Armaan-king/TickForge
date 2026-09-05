"""Events survive the round trip to Parquet, exactly.

"Exactly" is the whole point. Every assertion on a price here is equality, not
approximate equality -- an approximate check would pass against a float column
and quietly discard the guarantee the project is built on.
"""

import asyncio
import functools
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from tickforge.events import BookSnapshot, BookUpdate, Side, Trade
from tickforge.storage import EventStore, record

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
