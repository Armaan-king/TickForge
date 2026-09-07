"""Binance feed: the snapshot/buffer join, and the snapshot request itself.

No network. The join needs only sequence numbers, so its events carry no
levels; the HTTP layer runs against a real `httpx` client with a fake socket.
"""

import asyncio
import functools
import time
from collections.abc import AsyncGenerator, Sequence
from decimal import Decimal
from types import SimpleNamespace

import httpx
import pytest
from websockets.exceptions import WebSocketException

from tickforge.adapters import binance_feed
from tickforge.adapters.binance_feed import (
    BinanceFeed,
    ResyncRequired,
    SequenceGapError,
    SnapshotTooOldError,
    StaleFeedError,
    fetch_snapshot,
    require_fresh,
    updates_after_snapshot,
)
from tickforge.events import BookSnapshot, BookUpdate, Side, Trade


def async_test(fn):
    """Run an async test function on a fresh event loop.

    `functools.wraps` is load-bearing, not cosmetic: pytest reads the wrapped
    signature to decide which fixtures to inject, and reports the wrapped name
    when a test fails.

    Returns:
        A plain synchronous function with `fn`'s name and signature. Pytest
        calls that instead, so it never sees a coroutine.
    """

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        return asyncio.run(fn(*args, **kwargs))

    return wrapper


def update(first_seq: int, last_seq: int) -> BookUpdate:
    return BookUpdate("binance", "BTCUSDT", 0, 0, first_seq, last_seq, (), ())


def snapshot(last_seq: int) -> BookSnapshot:
    return BookSnapshot("binance", "BTCUSDT", 0, 0, last_seq, (), ())


def trade(trade_id: int) -> Trade:
    """Price and quantity are placeholders -- the feed never looks at them."""
    return Trade(
        "binance", "BTCUSDT", 0, 0, Decimal("1"), Decimal("1"), trade_id, Side.BUY
    )


def test_updates_entirely_before_the_snapshot_are_dropped() -> None:
    buffered = [update(400, 450), update(451, 499)]
    assert updates_after_snapshot(snapshot(500), buffered) == []


def test_update_spanning_the_boundary_is_kept() -> None:
    """The snapshot lands partway through an update's range.

    Filtering on first_seq instead of last_seq would drop this one and lose
    every change between the snapshot and the following update.
    """
    spanning = update(495, 505)
    assert updates_after_snapshot(snapshot(500), [spanning]) == [spanning]


def test_updates_after_the_snapshot_are_kept_in_order() -> None:
    buffered = [update(501, 510), update(511, 520)]
    assert updates_after_snapshot(snapshot(500), buffered) == buffered


def test_exactly_contiguous_buffer_is_accepted() -> None:
    """first_seq == snapshot.last_seq + 1 is the boundary, not a gap."""
    first = update(501, 510)
    assert updates_after_snapshot(snapshot(500), [first]) == [first]


def test_empty_buffer_is_not_an_error() -> None:
    assert updates_after_snapshot(snapshot(500), []) == []


def test_snapshot_newer_than_everything_buffered() -> None:
    """Routine: the snapshot won the race. The next live update continues
    from the snapshot's own sequence, so there is nothing to replay."""
    assert updates_after_snapshot(snapshot(600), [update(400, 500)]) == []


def test_gap_between_snapshot_and_buffer_raises() -> None:
    """Changes 501-509 exist somewhere but nobody caught them, so the
    snapshot and the buffer cannot be joined at all."""
    with pytest.raises(SnapshotTooOldError, match="510"):
        updates_after_snapshot(snapshot(500), [update(510, 520)])


def test_stale_updates_do_not_mask_a_real_gap() -> None:
    """The gap is measured from the oldest *surviving* update, not the
    oldest buffered one."""
    with pytest.raises(SnapshotTooOldError):
        updates_after_snapshot(snapshot(500), [update(400, 450), update(510, 520)])


def test_the_buffer_is_not_mutated() -> None:
    """_seed's caller still holds the buffer; the join must be a pure read."""
    buffered = [update(400, 450), update(495, 505)]
    original = list(buffered)
    updates_after_snapshot(snapshot(500), buffered)
    assert buffered == original


# --- fetch_snapshot ---------------------------------------------------------

SNAPSHOT_BODY = (
    '{"lastUpdateId":99588317538,'
    '"bids":[["77381.36000000","1.29993000"],["77381.35000000","0.00084000"]],'
    '"asks":[["77381.37000000","3.16444000"]]}'
)

BINANCE_ERROR_BODY = '{"code":-1121,"msg":"Invalid symbol."}'


def mock_client(
    *, status: int = 200, text: str = SNAPSHOT_BODY
) -> tuple[httpx.AsyncClient, list[httpx.Request]]:
    """A real `AsyncClient` with a fake socket, plus the requests it sent.

    Only the transport is replaced, so URL building, parameter encoding and
    `raise_for_status` all execute for real -- the tests exercise httpx's
    actual behaviour rather than an imitation of it.

    Returns:
        ``(client, seen)``. `seen` is a live reference to the list the
        handler appends to, not a copy: it is empty on return and holds one
        `httpx.Request` per call afterwards, so read it *after* the await.
        Every request gets the same canned response.
    """
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status, text=text)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler)), seen


@async_test
async def test_request_uses_uppercase_symbol_and_limit() -> None:
    """REST rejects a lowercase symbol; the stream rejects an uppercase one.

    `fetch_snapshot` owns the uppercasing so no caller has to remember which
    transport wants which case.
    """
    client, seen = mock_client()

    await fetch_snapshot(client, "btcusdt", limit=50)

    request = seen[0]
    assert request.url.path == "/api/v3/depth"
    assert request.url.params["symbol"] == "BTCUSDT"
    assert request.url.params["limit"] == "50"  # httpx returns params as text


@async_test
async def test_returns_a_parsed_snapshot() -> None:
    """The body reaches `parse_snapshot` and its result is returned unchanged.

    Level parsing is `test_binance_adapter.py`'s job -- asserting on it here
    would only duplicate coverage and break twice for one cause.
    """
    client, _ = mock_client()

    snap = await fetch_snapshot(client, "btcusdt")

    assert isinstance(snap, BookSnapshot)
    assert snap.exchange == "binance"
    assert snap.symbol == "BTCUSDT"
    assert snap.last_seq == 99588317538


@async_test
async def test_http_error_status_raises() -> None:
    """raise_for_status catches transport-level failure -- rate limits,
    outages, a wrong endpoint -- before the body reaches the parser."""
    client, _ = mock_client(status=429)

    with pytest.raises(httpx.HTTPStatusError):
        await fetch_snapshot(client, "BTCUSDT")


@async_test
async def test_binance_error_body_raises_despite_http_200() -> None:
    """The complement, and the reason both guards exist.

    Binance reports a bad symbol with HTTP 200 and an error object in the
    body. raise_for_status structurally cannot see that; parse_snapshot can.
    """
    client, _ = mock_client(text=BINANCE_ERROR_BODY)

    with pytest.raises(ValueError, match="Invalid symbol"):
        await fetch_snapshot(client, "NOPE")


@async_test
async def test_received_ns_is_stamped_at_call_time() -> None:
    """The timestamp is taken here, not left to parse_snapshot's default.

    Its default would fire later -- after raise_for_status and whatever else
    the event loop ran in between -- and that delay would be misread as feed
    latency. timestamp_ns mirrors it because REST carries no exchange time.
    """
    client, _ = mock_client()

    before = time.time_ns()
    snap = await fetch_snapshot(client, "BTCUSDT")
    after = time.time_ns()

    assert before <= snap.received_ns <= after
    assert snap.timestamp_ns == snap.received_ns


# --- the resync path --------------------------------------------------------
#
# None of this can be reached against the real exchange: a gap needs Binance to
# drop a frame. The state machine is driven from a scripted stream instead, and
# the interleaving is made deterministic by counting turns of the event loop --
# `asyncio` runs ready callbacks in FIFO order, so a fixed script always
# produces the same split between buffered and live updates.


class FakeStream:
    """Stand-in for `stream_updates`: a scripted async generator.

    Yields each item of a script in turn, raising it instead if it is an
    exception. One instance serves several connection attempts -- each call
    takes the next script and the last one repeats -- so a resync can be given
    different content from the attempt that failed.

    `log` records "connect" at the moment a real socket would open. Pass the
    same list to `FakeFetch` to assert the ordering between the two.
    """

    def __init__(
        self,
        *scripts: Sequence[BookUpdate | Exception],
        log: list[str] | None = None,
    ) -> None:
        self.scripts = scripts
        self.log = [] if log is None else log
        self.runs = 0
        self.closed = 0

    def __call__(self, symbol: str) -> AsyncGenerator[BookUpdate, None]:
        script = self.scripts[min(self.runs, len(self.scripts) - 1)]
        self.runs += 1
        return self._gen(script)

    async def _gen(
        self, script: Sequence[BookUpdate | Exception]
    ) -> AsyncGenerator[BookUpdate, None]:
        self.log.append("connect")
        try:
            for item in script:
                # One turn of the loop per frame, so the concurrent snapshot
                # fetch advances in a fixed, reproducible order.
                await asyncio.sleep(0)
                if isinstance(item, Exception):
                    raise item
                yield item
        finally:
            self.closed += 1


class FakeFetch:
    """Stand-in for `fetch_snapshot`.

    Returns each result in turn, raising it instead if it is an exception; the
    last result repeats.

    `hops` is how many turns of the event loop the request takes, which is what
    decides how many updates land in the buffer before `_seed` stops reading.
    `None` never completes, so the caller must cancel it.
    """

    def __init__(
        self,
        *results: BookSnapshot | Exception,
        hops: int | None = 1,
        log: list[str] | None = None,
    ) -> None:
        self.results = results
        self.hops = hops
        self.log = [] if log is None else log
        self.calls = 0
        self.cancelled = 0

    async def __call__(
        self, client: object, symbol: str, limit: int = 1000
    ) -> BookSnapshot:
        self.log.append("fetch")
        result = self.results[min(self.calls, len(self.results) - 1)]
        self.calls += 1
        try:
            if self.hops is None:
                await asyncio.Event().wait()
            else:
                for _ in range(self.hops):
                    await asyncio.sleep(0)
        except asyncio.CancelledError:
            self.cancelled += 1
            raise
        if isinstance(result, Exception):
            raise result
        return result


async def collect(events: AsyncGenerator, limit: int) -> list:
    """Drain up to `limit` events from a feed, then close it.

    Closing is the point: `run` never finishes on its own, and breaking out of
    the loop only suspends it. Its `finally` -- the one that closes the socket
    -- runs on `aclose`, not on going out of scope.

    Returns:
        The events drained, in order. Fewer than `limit` if the feed ended
        first.
    """
    out: list = []
    try:
        async for event in events:
            out.append(event)
            if len(out) >= limit:
                break
    finally:
        await events.aclose()
    return out


# --- _seed ------------------------------------------------------------------


@async_test
async def test_seed_opens_the_stream_before_fetching(monkeypatch) -> None:
    """The socket does not exist until the generator is first advanced.

    Creating the fetch task before that races the WebSocket handshake, and on
    a slow one the snapshot comes back newer than everything buffered -- the
    join then fails on every attempt and the feed rebuilds forever.
    """
    log: list[str] = []
    stream = FakeStream([update(101, 110), update(111, 120)], log=log)
    monkeypatch.setattr(
        binance_feed, "fetch_snapshot", FakeFetch(snapshot(100), log=log)
    )

    await BinanceFeed("BTCUSDT")._seed(None, stream("BTCUSDT"))

    assert log == ["connect", "fetch"]


@async_test
async def test_seed_buffers_while_the_snapshot_is_in_flight(monkeypatch) -> None:
    """The two must overlap. Fetch-then-buffer loses every update issued
    during the request; buffer-then-fetch reads nothing while it is in flight.
    """
    script = [update(101, 110), update(111, 120), update(121, 130), update(131, 140)]
    stream = FakeStream(script)
    monkeypatch.setattr(
        binance_feed, "fetch_snapshot", FakeFetch(snapshot(100), hops=2)
    )

    snap, buffered = await BinanceFeed("BTCUSDT")._seed(None, stream("BTCUSDT"))

    assert snap.last_seq == 100
    assert len(buffered) >= 2
    assert buffered == script[: len(buffered)]  # a prefix, in arrival order


@async_test
async def test_seed_cancels_the_fetch_when_buffering_fails(monkeypatch) -> None:
    """Otherwise the request runs on with nobody to collect its result or its
    exception, and asyncio reports it at some unrelated later moment."""
    stream = FakeStream([update(101, 110), WebSocketException("socket died")])
    fetch = FakeFetch(snapshot(100), hops=None)  # never completes on its own
    monkeypatch.setattr(binance_feed, "fetch_snapshot", fetch)

    with pytest.raises(WebSocketException):
        await BinanceFeed("BTCUSDT")._seed(None, stream("BTCUSDT"))

    assert fetch.cancelled == 1


# --- _sync_once -------------------------------------------------------------


@async_test
async def test_sync_once_emits_the_snapshot_then_every_update(monkeypatch) -> None:
    """A cold `OrderBook` consumes this stream as-is: load the snapshot, apply
    the rest in order. Where the buffered updates end and the live ones begin
    is invisible downstream, which is what makes the stream replayable."""
    script = [update(101, 110), update(111, 120), update(121, 130), update(131, 140)]
    monkeypatch.setattr(binance_feed, "stream_events", FakeStream(script))
    monkeypatch.setattr(
        binance_feed, "fetch_snapshot", FakeFetch(snapshot(100), hops=0)
    )

    events = await collect(BinanceFeed("BTCUSDT")._sync_once(None), 5)

    assert isinstance(events[0], BookSnapshot)
    assert events[0].last_seq == 100
    assert events[1:] == script


@async_test
async def test_sync_once_rejects_an_overlapping_update(monkeypatch) -> None:
    """Not a gap: 118 is *behind* the next expected sequence, so the old `>`
    comparison passed it straight through.

    `OrderBook.apply` then failed it as a GAP, went INVALID and stayed there,
    while the feed -- the only thing that can resynchronise -- streamed on
    unaware. Being laxer than the book is silent death.
    """
    script = [update(101, 110), update(111, 120), update(118, 135)]
    monkeypatch.setattr(binance_feed, "stream_events", FakeStream(script))
    monkeypatch.setattr(
        binance_feed, "fetch_snapshot", FakeFetch(snapshot(100), hops=0)
    )

    with pytest.raises(SequenceGapError, match="expected 121, got 118"):
        await collect(BinanceFeed("BTCUSDT")._sync_once(None), 10)


@async_test
async def test_sync_once_rejects_a_gap_inside_the_buffered_window(monkeypatch) -> None:
    """Buffered updates used to be emitted with no sequence check at all.

    A frame dropped between opening the socket and joining the snapshot went
    straight to the book -- the same silent death as a gap mid-stream, just in
    a window nobody was watching.
    """
    monkeypatch.setattr(
        binance_feed,
        "stream_events",
        FakeStream([update(101, 110), update(121, 130)]),
    )
    # Slow enough that both updates land in the buffer, so the live loop never
    # sees either of them.
    monkeypatch.setattr(
        binance_feed, "fetch_snapshot", FakeFetch(snapshot(100), hops=10)
    )

    with pytest.raises(SequenceGapError, match="expected 111, got 121"):
        await collect(BinanceFeed("BTCUSDT")._sync_once(None), 10)


@async_test
async def test_sync_once_accepts_a_spanning_update_after_an_empty_join(
    monkeypatch,
) -> None:
    """The snapshot won the race, so the first live update straddles it.

    Requiring exact contiguity here would reject the first legitimate update
    and rebuild forever -- the trap the book's SEEDED state exists to avoid.
    """
    script = [update(90, 95), update(96, 99), update(98, 115), update(116, 120)]
    monkeypatch.setattr(binance_feed, "stream_events", FakeStream(script))
    monkeypatch.setattr(
        binance_feed, "fetch_snapshot", FakeFetch(snapshot(100), hops=0)
    )

    events = await collect(BinanceFeed("BTCUSDT")._sync_once(None), 3)

    assert isinstance(events[0], BookSnapshot)
    assert events[1:] == script[2:]  # 90-99 predate the snapshot and are dropped


@async_test
async def test_trades_pass_through_without_a_sequence_check(monkeypatch) -> None:
    """A trade between two contiguous updates must not break the chain.

    Trades carry no sequence numbers at all, so a loop that checked every
    event uniformly would either raise on the missing `last_seq` or read the
    trade as a gap and resync on every execution.
    """
    script = [update(101, 110), update(111, 120), trade(1), update(121, 130)]
    monkeypatch.setattr(binance_feed, "stream_events", FakeStream(script))
    monkeypatch.setattr(
        binance_feed, "fetch_snapshot", FakeFetch(snapshot(100), hops=0)
    )

    events = await collect(BinanceFeed("BTCUSDT")._sync_once(None), 5)

    assert [type(e) for e in events] == [
        BookSnapshot,
        BookUpdate,
        BookUpdate,
        Trade,
        BookUpdate,
    ]


@async_test
async def test_trades_buffered_during_seeding_are_not_lost(monkeypatch) -> None:
    """They arrived on a live socket and describe real executions.

    Dropping them would punch a hole in trade flow at startup and after every
    single resync -- exactly the windows where the market is most interesting.
    """
    script = [trade(1), update(101, 110), update(111, 120)]
    monkeypatch.setattr(binance_feed, "stream_events", FakeStream(script))
    # Slow enough that the whole script lands in the buffer.
    monkeypatch.setattr(
        binance_feed, "fetch_snapshot", FakeFetch(snapshot(100), hops=10)
    )

    events = await collect(BinanceFeed("BTCUSDT")._sync_once(None), 4)

    assert [type(e) for e in events] == [
        BookSnapshot,
        Trade,
        BookUpdate,
        BookUpdate,
    ]


@async_test
async def test_sync_once_closes_the_stream_when_it_fails(monkeypatch) -> None:
    """The socket lives inside the generator, so only closing it releases the
    connection. A resync loop leaking one per attempt would run out."""
    stream = FakeStream([update(101, 110), update(121, 130)])
    monkeypatch.setattr(binance_feed, "stream_events", stream)
    monkeypatch.setattr(
        binance_feed, "fetch_snapshot", FakeFetch(snapshot(100), hops=10)
    )

    with pytest.raises(SequenceGapError):
        await collect(BinanceFeed("BTCUSDT")._sync_once(None), 10)

    assert stream.closed == 1


# --- staleness --------------------------------------------------------------


@async_test
async def test_a_silent_stream_is_stale_not_quiet(monkeypatch) -> None:
    """The failure a loop that only wakes on events cannot see.

    A socket that stays open and answers pings while delivering no data would
    otherwise leave the book serving its last state forever, as though it were
    current. `websockets` catches a dead TCP peer; nothing catches this.
    """

    async def silent():
        await asyncio.Event().wait()
        yield  # pragma: no cover -- never reached

    with pytest.raises(StaleFeedError):
        async for _ in require_fresh(silent(), timeout_s=0.01):
            pass


@async_test
async def test_a_live_stream_is_passed_through_untouched() -> None:
    """The guard must be invisible while data is flowing."""
    script = [update(101, 110), update(111, 120)]

    seen = [event async for event in require_fresh(FakeStream(script)("BTCUSDT"))]

    assert seen == script


@async_test
async def test_the_timeout_resets_on_every_event() -> None:
    """A slow feed is not a dead one. The deadline is per event, not per
    session, or any capture lasting longer than the timeout would fail."""

    async def slow():
        for seq in range(4):
            await asyncio.sleep(0.05)
            yield update(seq, seq)

    # 200ms of session against a 150ms timeout: the total exceeds it while no
    # single gap does. Margins are wide because Windows resolves sleeps to
    # ~15ms, so a tighter test would fail under suite contention rather than
    # on a real defect.
    seen = [event async for event in require_fresh(slow(), timeout_s=0.15)]

    assert len(seen) == 4


@async_test
async def test_the_wrapped_stream_is_closed() -> None:
    """Closing this generator does not close the one it wraps, and the socket
    lives in there. Same trap as `run` and `_sync_once`."""
    stream = FakeStream([update(101, 110)])

    async for _ in require_fresh(stream("BTCUSDT"), timeout_s=1):
        break

    # The break suspends the wrapper; aclose on it must reach the inner one.
    assert stream.closed == 0
    gen = require_fresh(stream("BTCUSDT"), timeout_s=1)
    await anext(gen)
    await gen.aclose()
    assert stream.closed == 1


@async_test
async def test_a_stale_feed_triggers_a_resync(monkeypatch) -> None:
    """StaleFeedError is a ResyncRequired, so `run` recovers from it the same
    way it recovers from a gap -- a fresh snapshot, no special case."""
    assert issubclass(StaleFeedError, ResyncRequired)


# --- run --------------------------------------------------------------------


@async_test
async def test_run_resynchronises_after_a_gap(monkeypatch) -> None:
    """Recovery is a second snapshot, not a repair.

    Downstream is never told a resync happened; applying the new snapshot is
    the whole of it, which is why the stream stays replayable.
    """
    monkeypatch.setattr(binance_feed, "RESYNC_DELAY_S", 0)
    stream = FakeStream(
        [update(101, 110), update(121, 130)],  # 111-120 never arrived
        [update(201, 210), update(211, 220)],  # clean second attempt
    )
    monkeypatch.setattr(binance_feed, "stream_events", stream)
    monkeypatch.setattr(
        binance_feed,
        "fetch_snapshot",
        FakeFetch(snapshot(100), snapshot(200), hops=0),
    )

    events = await collect(BinanceFeed("BTCUSDT").run(), 4)

    assert [type(e) for e in events] == [
        BookSnapshot,
        BookUpdate,
        BookSnapshot,
        BookUpdate,
    ]
    assert events[2].last_seq == 200
    assert stream.runs == 2
    assert stream.closed == 2  # both attempts released their socket


@async_test
async def test_run_waits_before_reconnecting_on_a_clean_close(monkeypatch) -> None:
    """`websockets` ends the iteration rather than raising when the server
    hangs up normally, so the wait cannot live in the except clause -- a
    server-side disconnect would spin the reconnect loop at full speed and
    earn an IP ban.
    """
    delays: list[float] = []

    async def spy_sleep(seconds: float) -> None:
        delays.append(seconds)

    # Only the attributes `binance_feed` actually uses, so the fakes above keep
    # the real `asyncio.sleep` and their event-loop hops still work.
    monkeypatch.setattr(
        binance_feed,
        "asyncio",
        SimpleNamespace(
            sleep=spy_sleep,
            create_task=asyncio.create_task,
            wait_for=asyncio.wait_for,
            CancelledError=asyncio.CancelledError,
        ),
    )
    stream = FakeStream([update(101, 110), update(111, 120)])
    monkeypatch.setattr(binance_feed, "stream_events", stream)
    monkeypatch.setattr(
        binance_feed,
        "fetch_snapshot",
        FakeFetch(snapshot(100), snapshot(200), hops=0),
    )

    events = await collect(BinanceFeed("BTCUSDT").run(), 4)

    assert delays == [binance_feed.RESYNC_DELAY_S]
    assert [type(e) for e in events] == [
        BookSnapshot,
        BookUpdate,
        BookUpdate,
        BookSnapshot,
    ]
    assert stream.runs == 2
