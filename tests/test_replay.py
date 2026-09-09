"""Recorded events come back identical, in order, and replay the same twice.

The determinism test is success criterion 6 and the project's headline claim:
live and recorded data traverse one pipeline, and repeating a replay produces
the same output.
"""

import asyncio
import functools
import time
from decimal import Decimal

import pytest

from tickforge.analytics import FlowFeatures, feature_snapshot
from tickforge.book import ApplyResult, OrderBook
from tickforge.events import BookSnapshot, BookUpdate, Side, Trade
from tickforge.replay import read_partition, replay, replay_partition
from tickforge.storage import EventStore

T0 = 1_788_307_200_000_000_000  # 2026-09-02 00:00:00 UTC
DAY = "2026-09-02"
SECOND = 1_000_000_000
WINDOW = 60 * SECOND


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
        levels((("100", "2"), ("99", "6"), ("98", "8"))),
        levels((("101", "6"), ("102", "6"), ("103", "4"))),
    )


def update(seq, at=T0, bids=(), asks=()) -> BookUpdate:
    return BookUpdate(
        "binance", "BTCUSDT", at, at, seq, seq, levels(bids), levels(asks)
    )


def trade(at=T0, price="100.5", quantity="0.25", side=Side.BUY, trade_id=1) -> Trade:
    return Trade(
        "binance", "BTCUSDT", at, at, Decimal(price), Decimal(quantity), trade_id, side
    )


SESSION = [
    snapshot(at=T0),
    trade(at=T0 + 1, trade_id=1),
    update(101, at=T0 + 2, bids=(("100", "9"),)),
    trade(at=T0 + 3, trade_id=2, side=Side.SELL),
    update(102, at=T0 + 4, asks=(("101", "3"),)),
    update(103, at=T0 + 5, bids=(("100.5", "4"),)),
]


def capture(root, events=SESSION, session=1) -> None:
    with EventStore(root, "binance", "BTCUSDT", session=session) as store:
        for event in events:
            store.write(event)


def drive(events):
    """Run events through a book and a window, collecting every feature row.

    Mirrors what __main__ does, so the assertions below are about the pipeline
    a user actually gets, not a simplification of it.
    """
    book = OrderBook("binance", "BTCUSDT")
    flow = FlowFeatures(WINDOW)
    rows = []
    for event in events:
        if isinstance(event, BookSnapshot):
            book.load_snapshot(event)
            if not book.is_valid:
                continue
        elif isinstance(event, BookUpdate):
            if book.apply(event) is not ApplyResult.APPLIED:
                continue
        flow.observe(event, book)
        if isinstance(event, BookUpdate):
            rows.append(feature_snapshot(book, flow, event.timestamp_ns))
    return rows


# --- round trip -------------------------------------------------------------


def test_every_event_comes_back_identical(tmp_path) -> None:
    """Dataclass equality, so this compares every field including Decimals.

    Reading rescales a price to 18 places -- a different representation of the
    same value. It compares equal, and Python hashes Decimals by value, so the
    replayed price is the same order-book dict key.
    """
    capture(tmp_path)
    assert read_partition(tmp_path, "binance", "BTCUSDT", DAY) == SESSION


def test_a_rolled_capture_replays_in_one_stream(tmp_path) -> None:
    """A long capture rolls its files so a kill cannot lose the whole run.

    Replay has to stitch them back into one ordered stream, which works because
    the session stamps sort and `capture_seq` stays dense across the roll.
    """
    long_session = [snapshot()] + [
        trade(at=T0 + n, trade_id=n) if n % 3 == 0 else update(100 + n, at=T0 + n)
        for n in range(1, 25)
    ]
    with EventStore(tmp_path, "binance", "BTCUSDT", roll_rows=5) as store:
        for event in long_session:
            store.write(event)

    directory = tmp_path / "binance" / "BTCUSDT" / DAY
    assert len(list(directory.glob("*.parquet"))) > 3  # it really did roll
    assert read_partition(tmp_path, "binance", "BTCUSDT", DAY) == long_session


def test_zero_quantities_survive(tmp_path) -> None:
    """A zero quantity is a delete. Dropping one on the way back would leave
    the replayed book holding a level the exchange removed."""
    deleting = [snapshot(), update(101, at=T0 + 1, bids=(("99", "0"),))]
    capture(tmp_path, deleting)

    recovered = read_partition(tmp_path, "binance", "BTCUSDT", DAY)
    assert recovered[1].bids == ((Decimal("99"), Decimal("0")),)


def test_features_are_not_replayed(tmp_path) -> None:
    """They are derived output. Replaying them instead of recomputing would
    defeat the point of proving the computation is reproducible."""
    with EventStore(tmp_path, "binance", "BTCUSDT", session=1) as store:
        store.write(snapshot())
        store.write_features(drive([snapshot(), update(101, at=T0 + 1)])[0])

    recovered = read_partition(tmp_path, "binance", "BTCUSDT", DAY)
    assert len(recovered) == 1
    assert isinstance(recovered[0], BookSnapshot)


def test_a_missing_partition_is_loud(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        read_partition(tmp_path, "binance", "BTCUSDT", "1999-01-01")


# --- ordering ---------------------------------------------------------------


def test_order_survives_colliding_timestamps(tmp_path) -> None:
    """The reason capture_seq exists.

    Every event here shares one timestamp_ns and one received_ns, which is not
    contrived: time.time_ns() resolves to ~0.6ms on the machine this was
    measured on, so about 3% of real captured events collide. Ordering on
    either timestamp column would be arbitrary here; capture_seq is exact.
    """
    identical = [
        snapshot(at=T0),
        trade(at=T0, trade_id=1),
        update(101, at=T0, bids=(("100", "9"),)),
        trade(at=T0, trade_id=2),
        update(102, at=T0, bids=(("100", "7"),)),
    ]
    capture(tmp_path, identical)

    assert read_partition(tmp_path, "binance", "BTCUSDT", DAY) == identical


def test_sessions_replay_in_order(tmp_path) -> None:
    capture(tmp_path, [snapshot(), update(101, at=T0 + 1)], session=1)
    capture(tmp_path, [snapshot(at=T0 + 10), update(201, at=T0 + 11)], session=2)

    recovered = read_partition(tmp_path, "binance", "BTCUSDT", DAY)
    assert [type(event).__name__ for event in recovered] == [
        "BookSnapshot", "BookUpdate", "BookSnapshot", "BookUpdate",
    ]
    assert recovered[1].first_seq == 101
    assert recovered[3].first_seq == 201


# --- determinism ------------------------------------------------------------


@async_test
async def test_replaying_twice_gives_identical_output(tmp_path) -> None:
    """Success criterion 6, and the claim the whole architecture rests on."""
    capture(tmp_path)

    first = drive([e async for e in replay_partition(tmp_path, "binance", "BTCUSDT", DAY)])
    second = drive([e async for e in replay_partition(tmp_path, "binance", "BTCUSDT", DAY)])

    assert first == second
    assert first  # and it actually produced rows


@async_test
async def test_replay_matches_the_events_it_recorded(tmp_path) -> None:
    """The stronger claim: a capture and its replay drive the pipeline to the
    same state. Storage round trip and replay ordering together."""
    capture(tmp_path)

    live = drive(SESSION)
    replayed = drive([e async for e in replay_partition(tmp_path, "binance", "BTCUSDT", DAY)])

    assert live == replayed


@async_test
async def test_speed_does_not_change_the_output(tmp_path) -> None:
    """Pacing moves when an event appears, never what it is. That separation
    is what lets wall-clock timing exist in the pipeline at all."""
    capture(tmp_path)
    events = read_partition(tmp_path, "binance", "BTCUSDT", DAY)

    fast = [event async for event in replay(events, speed=0)]
    paced = [event async for event in replay(events, speed=1_000_000)]

    assert fast == paced == events


# --- pacing -----------------------------------------------------------------


@async_test
async def test_unpaced_replay_does_not_sleep() -> None:
    spread_out = [trade(at=T0 + n * 10 * SECOND, trade_id=n) for n in range(5)]

    before = time.perf_counter()
    drained = [event async for event in replay(spread_out, speed=0)]
    elapsed = time.perf_counter() - before

    assert len(drained) == 5
    assert elapsed < 0.1  # 40 seconds of event time, no waiting


@async_test
async def test_pacing_waits_proportionally() -> None:
    """Two events 200ms apart at 4x should take about 50ms."""
    pair = [trade(at=T0), trade(at=T0 + 200_000_000, trade_id=2)]

    before = time.perf_counter()
    async for _ in replay(pair, speed=4):
        pass
    elapsed = time.perf_counter() - before

    assert 0.02 < elapsed < 0.4  # loose: asyncio.sleep only guarantees a floor


@async_test
async def test_a_backwards_clock_does_not_sleep() -> None:
    """time.time_ns() can step backwards, and a negative delay raises rather
    than simply not waiting."""
    backwards = [trade(at=T0 + SECOND), trade(at=T0, trade_id=2)]

    before = time.perf_counter()
    drained = [event async for event in replay(backwards, speed=1)]
    elapsed = time.perf_counter() - before

    assert len(drained) == 2
    assert elapsed < 0.1
