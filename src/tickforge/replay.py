"""Deterministic replay of recorded captures.

Reads a partition back into `MarketEvent`s and yields them at the same seam
`BinanceFeed.run()` yields at, so live and recorded data traverse one pipeline.
Nothing downstream can tell which it is receiving -- that indistinguishability
is the point, and `architecture.md` says what breaks without it.

Design and the measurements that forced it:
docs/superpowers/specs/2026-09-06-deterministic-replay-design.md
"""

import asyncio
import re
from collections.abc import AsyncIterator, Iterable
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from tickforge.events import BookSnapshot, BookUpdate, MarketEvent, Side, Trade
from tickforge.storage import NS_PER_SECOND

SESSION_FILE = re.compile(r"^(?P<stream>[a-z_]+)-(?P<session>\d+)\.parquet$")
"""``trades-1788682769.parquet``. The stamp separates capture sessions."""


def _levels(structs: list[dict]) -> tuple[tuple[Decimal, Decimal], ...]:
    """Structs back into `PriceLevel` pairs -- the inverse of storage's.

    Quantities of zero are kept: they are deletions, and dropping one here
    would leave the replayed book holding a level the exchange removed.
    """
    return tuple((level["price"], level["quantity"]) for level in structs)


def _event(row: dict, stream: str) -> MarketEvent:
    """One stored row back into the event that produced it.

    The inverse of `EventStore._row`. Reading a price back rescales it to 18
    decimal places, which is a different representation of the same value --
    and Python hashes Decimals by value, so the replayed price is the same
    order-book dict key. Nothing needs converting.
    """
    common = (row["exchange"], row["symbol"], row["timestamp_ns"], row["received_ns"])
    if stream == "trades":
        return Trade(
            *common,
            price=row["price"],
            quantity=row["quantity"],
            trade_id=row["trade_id"],
            aggressor=Side(row["aggressor"]),
        )
    if stream == "snapshots":
        return BookSnapshot(
            *common,
            last_seq=row["last_seq"],
            bids=_levels(row["bids"]),
            asks=_levels(row["asks"]),
        )
    return BookUpdate(
        *common,
        first_seq=row["first_seq"],
        last_seq=row["last_seq"],
        bids=_levels(row["bids"]),
        asks=_levels(row["asks"]),
    )


def _session_files(directory: Path) -> dict[int, dict[str, Path]]:
    """A partition's files grouped by capture session.

    Returns:
        ``{session_stamp: {stream: path}}``. Features are excluded -- they are
        derived output, and replaying them would defeat the point of
        recomputing them.
    """
    sessions: dict[int, dict[str, Path]] = {}
    for path in directory.glob("*.parquet"):
        match = SESSION_FILE.match(path.name)
        if match is None or match["stream"] == "features":
            continue
        sessions.setdefault(int(match["session"]), {})[match["stream"]] = path
    return sessions


def read_partition(
    root: Path | str, exchange: str, symbol: str, date: str
) -> list[MarketEvent]:
    """Every recorded event for one venue, symbol and UTC day, in order.

    Ordered by ``(session, capture_seq)``. No timestamp participates: on the
    machine this was measured on, `time.time_ns()` resolves to ~0.6ms so about
    3% of events share a `received_ns`, and the exchange clock leads far enough
    that an event's `timestamp_ns` can precede its own arrival. The two columns
    produce different orderings and both have ties, so only the counter the
    store assigned at write time can reproduce the stream.

    Raises:
        FileNotFoundError: No capture exists for that partition.
    """
    directory = Path(root) / exchange / symbol / date
    sessions = _session_files(directory) if directory.is_dir() else {}
    if not sessions:
        raise FileNotFoundError(f"no capture at {directory}")

    events: list[MarketEvent] = []
    # ponytail: reads a whole partition into memory. A streaming heap merge
    # across the three files is the upgrade if a capture outgrows RAM.
    for session in sorted(sessions):
        rows: list[tuple[int, MarketEvent]] = []
        for stream, path in sessions[session].items():
            for row in pq.read_table(path).to_pylist():
                rows.append((row["capture_seq"], _event(row, stream)))
        rows.sort(key=lambda pair: pair[0])
        events.extend(event for _, event in rows)
    return events


async def replay(
    events: Iterable[MarketEvent], speed: float = 0
) -> AsyncIterator[MarketEvent]:
    """Yield recorded events at the seam a live feed yields at.

    Args:
        speed: 0 replays as fast as possible. Otherwise each event waits its
            `received_ns` gap from the previous one, divided by speed -- so 1
            is real time and 100 is a hundred times faster. The arrival rhythm
            rather than the exchange-stamped one, because that is what the live
            experience was.

    Yields:
        The recorded `MarketEvent`s, unmodified and in order.

    The only wall clock in the pipeline lives here, and `pitfalls.md` permits
    exactly this: pacing changes *when* output appears, never what it is. Two
    replays at different speeds produce identical output.
    """
    previous: int | None = None
    for event in events:
        if speed and previous is not None:
            # Clamped: time.time_ns() can step backwards, and asyncio.sleep
            # raises on a negative delay rather than returning immediately.
            gap = max(0, event.received_ns - previous) / NS_PER_SECOND / speed
            if gap:
                await asyncio.sleep(gap)
        previous = event.received_ns
        yield event


def replay_partition(
    root: Path | str, exchange: str, symbol: str, date: str, speed: float = 0
) -> AsyncIterator[MarketEvent]:
    """Read a partition and replay it. The whole of Phase 6 in one call."""
    return replay(read_partition(root, exchange, symbol, date), speed)
