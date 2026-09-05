"""Binance depth feed: connection, resynchronisation, event emission.

Implements Binance's documented order-book procedure. The feed emits a
``MarketEvent`` stream that a cold `OrderBook` can consume directly -- it does
not own a book itself, so the same stream can be recorded and replayed.
"""

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import suppress

import httpx
import websockets
from websockets.exceptions import WebSocketException

from tickforge.adapters.binance import SNAPSHOT_URL, parse_snapshot, parse_stream_frame
from tickforge.events import BookSnapshot, BookUpdate, MarketEvent, Trade

STREAM_URL = "wss://stream.binance.com:9443/stream"
RESYNC_DELAY_S = 1.0
"""Pause before rebuilding. Connection health only -- never affects output."""

class ResyncRequired(RuntimeError):
    """The book cannot be maintained from here; a fresh snapshot is needed."""

class SnapshotTooOldError(ResyncRequired):
    """The snapshot predates the buffered updates; fetch a fresh one."""

class SequenceGapError(ResyncRequired):
    """Updates were missed mid-stream."""

def updates_after_snapshot(snapshot:BookSnapshot,buffered:list[BookUpdate])->list[BookUpdate]:
    """Discard the buffered updates the snapshot already reflects.

    Filtering is on ``last_seq``, so the update whose range *spans* the
    snapshot boundary survives -- part of it is still new.

    Returns:
        A new list, oldest first, of the updates still to be applied. Empty
        when the snapshot is newer than everything buffered, which is normal.
        The input list is never modified.

    Raises:
        SnapshotTooOldError: Updates are missing between the snapshot and the
            start of the buffer, so the two cannot be joined at all.
    """
    survivors =[u for u in buffered if u.last_seq>snapshot.last_seq]
    if not survivors:
        return survivors

    if survivors[0].first_seq>snapshot.last_seq+1:
        raise SnapshotTooOldError(
            f"snapshot ends at {snapshot.last_seq},"
            f"buffer starts at {survivors[0].first_seq}"
        )
    return survivors

async def fetch_snapshot(client: httpx.AsyncClient, symbol: str, limit: int=1000) -> BookSnapshot:
    """Fetch a depth snapshot for one symbol.

    Args:
        client: Reused across resyncs, so the caller owns its lifetime.
        symbol: Any case. Uppercased here because REST requires it -- the
            WebSocket stream requires lowercase, so neither caller has to know.
        limit: Levels per side. 1000 is the deepest at normal request weight.

    Returns:
        The snapshot, stamped with the local time the response arrived.
        ``timestamp_ns`` mirrors ``received_ns``: REST carries no exchange time.

    Raises:
        httpx.HTTPStatusError: Transport-level failure -- rate limit, outage.
        ValueError: HTTP 200 carrying a Binance error object, which
            ``raise_for_status`` structurally cannot detect.
    """
    symbol=symbol.upper()
    response=await client.get(
        SNAPSHOT_URL,
        params={"symbol":symbol,"limit":limit}
    )
    response.raise_for_status()
    received_ns=time.time_ns()
    return parse_snapshot(
        response.text,
        symbol,
        received_ns=received_ns
    )

async def stream_events(symbol:str)->AsyncIterator[BookUpdate | Trade]:
    """Yield normalized depth updates and trades from Binance's WebSocket.

    Yields:
        One `BookUpdate` or `Trade` per frame, stamped with the moment it
        arrived, interleaved exactly as the venue sent them.

    Both subscriptions share one socket. Two connections would mean inventing
    an ordering between trades and depth updates that the exchange never
    stated, and Phase 4 reads trade flow against book state -- so that
    ordering has to come from the venue, not from whichever `asyncio` task
    happened to be scheduled first.

    The socket lives inside this generator, so it stays open while the
    generator is merely paused and closes when the generator is closed. One
    connection only: reconnection means the book is stale, which is the
    coordinator's problem, not this function's.
    """
    symbol=symbol.lower()
    new_url=f"{STREAM_URL}?streams={symbol}@depth/{symbol}@trade"
    async with websockets.connect(new_url) as ws:
        async for message in ws:
            received_ns=time.time_ns()
            yield parse_stream_frame(message,received_ns=received_ns)

class BinanceFeed:
    """Maintains a synchronized Binance depth feed for one symbol.

    Emits a stream a cold `OrderBook` can consume as-is: a snapshot, then
    contiguous updates, then a fresh snapshot each time synchronisation is
    lost. Downstream is never told a resync happened -- applying the new
    snapshot *is* the recovery.
    """

    def __init__(self, symbol:str)->None:
        self.symbol=symbol

    async def run(self)->AsyncIterator[MarketEvent]:
        """Emit a gap-free event stream, resynchronising as needed.

        Never returns. One HTTP client spans every resync, so rebuilding
        reuses the connection instead of repeating the TLS handshake.

        Yields:
            `BookSnapshot` first, then `BookUpdate`s contiguous with it. A
            further `BookSnapshot` appears wherever a resync happened -- that
            is the only signal, and applying it is the whole recovery.
        """
        async with httpx.AsyncClient() as client:
            while True:
                session = self._sync_once(client)
                try:
                    async for event in session:
                        yield event
                except (ResyncRequired, WebSocketException, httpx.HTTPError, OSError):
                    pass
                finally:
                    # Abandoning the iteration -- because a resync is due, or
                    # because the consumer stopped reading -- does not close
                    # the generator, and its `finally` is what releases the
                    # socket. Left to the garbage collector it may never run.
                    await session.aclose()
                # Reached on a clean close too: `websockets` ends the iteration
                # rather than raising when the server hangs up normally, so an
                # except-only wait would spin the reconnect loop at full speed.
                # Retrying instantly through an outage gets the IP banned.
                await asyncio.sleep(RESYNC_DELAY_S)

    async def _sync_once(self, client: httpx.AsyncClient) -> AsyncIterator[MarketEvent]:
        """One synchronised session: seed, join, then stream until it breaks.

        Yields:
            Exactly one `BookSnapshot`, then `BookUpdate`s in sequence order
            with `Trade`s interleaved. A session normally ends by raising; a
            clean socket close ends it without one.

        Raises:
            ResyncRequired: Synchronisation was lost and cannot be recovered
                without a new snapshot.
        """
        stream = stream_events(self.symbol)
        try:
            snapshot, buffered = await self._seed(client, stream)
            # Trades have no sequence relationship to the snapshot, so they
            # are neither filtered against it nor checked for contiguity.
            trades = [e for e in buffered if isinstance(e, Trade)]
            joined = updates_after_snapshot(
                snapshot, [e for e in buffered if isinstance(e, BookUpdate)]
            )

            yield snapshot

            # Emitted as a block rather than in arrival order. Nothing
            # downstream can observe the difference -- a trade is not ordered
            # against a sequence number -- and it only affects the one-second
            # seeding window. Interleaving them properly would mean tracking
            # each update's position in `buffered` for no gain.
            for trade in trades:
                yield trade

            last_seq = snapshot.last_seq
            synced = False

            # Buffered and live updates get the same check. A gap that opened
            # during the buffering window is exactly as fatal as one that opens
            # later, and only the live loop used to be checked at all.
            for update in joined:
                if update.last_seq <= last_seq:
                    continue
                self._require_contiguous(update, last_seq, synced)
                yield update
                last_seq, synced = update.last_seq, True

            async for event in stream:
                if isinstance(event, Trade):
                    yield event
                    continue
                if event.last_seq <= last_seq:
                    continue
                self._require_contiguous(event, last_seq, synced)
                yield event
                last_seq, synced = event.last_seq, True
        finally:
            # Closes the generator's `async with`, and with it the socket.
            await stream.aclose()

    def _require_contiguous(
        self, update: BookUpdate, last_seq: int, synced: bool
    ) -> None:
        """Reject any update the book would reject.

        Mirrors `OrderBook.apply` deliberately. The two checks exist for
        opposite purposes -- the book fails closed, this one triggers recovery
        -- but they must agree on what "contiguous" means. Being laxer than the
        book is the dangerous direction: the book would go INVALID while the
        feed, the only thing that can resynchronise, streams on unaware.

        Args:
            last_seq: Last sequence number emitted so far.
            synced: False until the first update after the snapshot has been
                emitted, mirroring the book's SEEDED state. While False an
                update may *span* the snapshot boundary rather than start
                exactly at it.

        Raises:
            SequenceGapError: The update cannot follow `last_seq`.
        """
        expected = last_seq + 1
        contiguous = (
            update.first_seq <= expected if not synced else update.first_seq == expected
        )
        if not contiguous:
            raise SequenceGapError(
                f"{self.symbol}: expected {expected}, got {update.first_seq}"
            )

    async def _seed(
        self,
        client: httpx.AsyncClient,
        stream: AsyncIterator[BookUpdate | Trade],
    ) -> tuple[BookSnapshot, list[BookUpdate | Trade]]:
        """Buffer events while the snapshot is fetched concurrently.

        The two must overlap. Fetching first loses every update issued during
        the request; buffering first without concurrency means nobody reads the
        socket while the request is in flight. The snapshot lands partway
        through the buffered range and `updates_after_snapshot` joins them.

        Returns:
            ``(snapshot, buffered)`` -- the snapshot, and every event that
            arrived while it was being fetched, oldest first and unfiltered.
            Never empty, and mixed: trades and updates in arrival order.
            Buffered updates older than the snapshot are still present; the
            caller discards them.
        """
        # The socket does not exist until the generator is first advanced, so
        # this line is what connects. Creating the fetch task before it races
        # the handshake, and on a slow one the snapshot comes back newer than
        # anything buffered -- which fails the join on every attempt, forever.
        buffered = [await anext(stream)]
        snapshot_task = asyncio.create_task(fetch_snapshot(client, self.symbol))
        try:
            async for update in stream:
                buffered.append(update)
                if snapshot_task.done():
                    break
            return await snapshot_task, buffered
        finally:
            # A failure in the buffering loop would otherwise leave the fetch
            # running with nobody to collect its result or its exception.
            if not snapshot_task.done():
                snapshot_task.cancel()
                with suppress(asyncio.CancelledError):
                    await snapshot_task