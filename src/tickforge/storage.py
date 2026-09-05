"""Parquet persistence for the normalized event stream.

Records raw events, not derived state. Features can always be recomputed from
raw events; raw events cannot be recovered from features. That asymmetry is
what makes history re-derivable when book or analytics logic changes -- see
docs/knowledge/architecture.md -- and it is what Phase 6 replays.

Design and rejected alternatives:
docs/superpowers/specs/2026-09-05-parquet-storage-design.md
"""

import datetime as dt
from collections.abc import AsyncIterator
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from tickforge.events import BookSnapshot, MarketEvent, PriceLevel, Trade

NS_PER_SECOND = 1_000_000_000

PRICE = pa.decimal128(38, 18)
"""Prices and quantities, exact.

Not a float. `decisions.md` records why this project is Decimal end to end:
exchanges quote decimal strings precisely because IEEE-754 cannot represent
tick sizes, and price is a dictionary key in the order book. Writing doubles
would discard that at the moment the data becomes permanent.

38 digits with 18 after the point covers every venue's tick size with room for
a price in the trillions.

Reading back rescales to 18 places: `Decimal('77381.36000000')` returns as
`Decimal('77381.360000000000000000')`. Numerically identical, different
`as_tuple()`. That is safe only because Python hashes Decimals by *value*, so
the rescaled price is the same dict key in the order book. Were it otherwise,
every replayed level would land on a fresh key and the book would silently
double.
"""

LEVELS = pa.list_(pa.struct([("price", PRICE), ("quantity", PRICE)]))
"""One side of a book event, nested rather than exploded into its own rows.

An update is atomic -- its sequence range describes the whole batch -- so
flattening to one row per level would multiply rows by roughly 500 and force
every reader to reconstruct the boundaries before it could replay anything.
"""

# `exchange` and `symbol` repeat the directory path on every row. Parquet
# dictionary-encodes a constant string column down to one dictionary entry plus
# RLE indices, so the cost is negligible -- and it keeps a file readable on its
# own if it is ever moved out of its partition.
_IDENTITY = [("exchange", pa.string()), ("symbol", pa.string())]

# int64 nanoseconds rather than pa.timestamp('ns'): the events already carry
# integers, and the timestamp type adds timezone semantics plus a conversion
# that can only lose information.
_TIMES = [("timestamp_ns", pa.int64()), ("received_ns", pa.int64())]

TRADE_SCHEMA = pa.schema(
    _IDENTITY
    + _TIMES
    + [
        ("trade_id", pa.int64()),
        ("price", PRICE),
        ("quantity", PRICE),
        # The aggressor Side, as its string value. This is what StrEnum was
        # chosen for -- the member *is* the string, so it lands here with no
        # conversion step and survives the round trip unchanged.
        ("aggressor", pa.string()),
    ]
)

UPDATE_SCHEMA = pa.schema(
    _IDENTITY
    + _TIMES
    + [
        ("first_seq", pa.int64()),
        ("last_seq", pa.int64()),
        ("bids", LEVELS),
        ("asks", LEVELS),
    ]
)

SNAPSHOT_SCHEMA = pa.schema(
    _IDENTITY
    + _TIMES
    + [
        # No range: a snapshot is the whole book as of one sequence number, not
        # a change spanning several. Sharing the update schema would mean
        # nullable range columns and a row whose meaning depends on which
        # columns happen to be null.
        ("last_seq", pa.int64()),
        ("bids", LEVELS),
        ("asks", LEVELS),
    ]
)


def _date_of(timestamp_ns: int) -> str:
    """The UTC date a partition is keyed by, as ``YYYY-MM-DD``.

    Derived from the *event's* timestamp, never the wall clock: replaying last
    week's capture has to write into last week's partition. Integer division
    before the conversion, because nanoseconds do not survive a float.
    """
    seconds = timestamp_ns // NS_PER_SECOND
    return dt.datetime.fromtimestamp(seconds, tz=dt.UTC).strftime("%Y-%m-%d")


def _levels(levels: tuple[PriceLevel, ...]) -> list[dict[str, Decimal]]:
    """Turn ``(price, quantity)`` pairs into the structs LEVELS expects."""
    return [{"price": price, "quantity": quantity} for price, quantity in levels]


STREAMS = {
    "trades": TRADE_SCHEMA,
    "book_updates": UPDATE_SCHEMA,
    "snapshots": SNAPSHOT_SCHEMA,
}
"""File name to schema. One Parquet file per stream per partition."""


class EventStore:
    """Writes a normalized event stream to date-partitioned Parquet.

    One directory per venue, symbol and UTC day; one file per event type::

        <root>/binance/BTCUSDT/2026-09-05/trades.parquet
                                         /book_updates.parquet
                                         /snapshots.parquet

    Rows are buffered and written as row groups, because a Parquet file is
    columnar: writing one row at a time would make every row its own row group,
    each with full footer overhead.

    Use as a context manager. An unclosed Parquet file has no footer and cannot
    be read at all, so closing is a correctness requirement rather than
    tidiness.
    """

    def __init__(
        self,
        root: Path | str,
        exchange: str,
        symbol: str,
        batch_rows: int = 5_000,
        batch_ns: int = 30 * NS_PER_SECOND,
    ) -> None:
        """
        Args:
            root: Directory the partition tree is built under.
            batch_rows: Flush once any stream reaches this many buffered rows.
            batch_ns: Flush once this much *event* time has passed since the
                last flush. Row count alone leaves a half-full buffer unwritten
                indefinitely on a quiet symbol; an interval alone gives wildly
                uneven row groups on a busy one. Both bound the loss window
                regardless of feed rate.
        """
        self._root = Path(root)
        self._exchange = exchange
        self._symbol = symbol
        self._batch_rows = batch_rows
        self._batch_ns = batch_ns
        self._date: str | None = None
        self._writers: dict[str, pq.ParquetWriter] = {}
        self._buffers: dict[str, list[dict]] = {name: [] for name in STREAMS}
        self._last_flush_ns: int | None = None

    def __enter__(self) -> "EventStore":
        return self

    def __exit__(self, *_) -> None:
        self.close()

    def write(self, event: MarketEvent) -> None:
        """Buffer one event, rolling the partition and flushing as needed."""
        date = _date_of(event.timestamp_ns)
        if date != self._date:
            self._roll(date)
        if self._last_flush_ns is None:
            self._last_flush_ns = event.timestamp_ns

        stream, row = self._row(event)
        self._buffers[stream].append(row)

        if self._due(event.timestamp_ns):
            self.flush()
            self._last_flush_ns = event.timestamp_ns

    def flush(self) -> None:
        """Write every buffered row out as a row group."""
        for name, rows in self._buffers.items():
            if not rows:
                continue
            table = pa.Table.from_pylist(rows, schema=STREAMS[name])
            self._writer(name).write_table(table)
            rows.clear()

    def close(self) -> None:
        """Flush and close the footers. Idempotent."""
        self.flush()
        self._close_writers()

    def _row(self, event: MarketEvent) -> tuple[str, dict]:
        """Which stream this event belongs to, and its row."""
        common = {
            "exchange": self._exchange,
            "symbol": self._symbol,
            "timestamp_ns": event.timestamp_ns,
            "received_ns": event.received_ns,
        }
        if isinstance(event, Trade):
            return "trades", {
                **common,
                "trade_id": event.trade_id,
                "price": event.price,
                "quantity": event.quantity,
                # A StrEnum member *is* its string, so it needs no conversion.
                "aggressor": event.aggressor,
            }
        if isinstance(event, BookSnapshot):
            return "snapshots", {
                **common,
                "last_seq": event.last_seq,
                "bids": _levels(event.bids),
                "asks": _levels(event.asks),
            }
        return "book_updates", {
            **common,
            "first_seq": event.first_seq,
            "last_seq": event.last_seq,
            "bids": _levels(event.bids),
            "asks": _levels(event.asks),
        }

    def _due(self, now_ns: int) -> bool:
        """Whether either bound has been reached."""
        if any(len(rows) >= self._batch_rows for rows in self._buffers.values()):
            return True
        return now_ns - (self._last_flush_ns or now_ns) >= self._batch_ns

    def _writer(self, name: str) -> pq.ParquetWriter:
        """The open writer for one stream, creating the partition on demand.

        Lazily, so a session that sees no trades leaves no empty trades file to
        confuse a reader into thinking none occurred rather than none were
        recorded.
        """
        if name not in self._writers:
            directory = self._root / self._exchange / self._symbol / str(self._date)
            directory.mkdir(parents=True, exist_ok=True)
            self._writers[name] = pq.ParquetWriter(
                directory / f"{name}.parquet", STREAMS[name]
            )
        return self._writers[name]

    def _roll(self, date: str) -> None:
        """Close the old partition and point at the new one."""
        self.flush()
        self._close_writers()
        self._date = date

    def _close_writers(self) -> None:
        for writer in self._writers.values():
            writer.close()
        self._writers.clear()


async def record(
    events: AsyncIterator[MarketEvent], store: EventStore
) -> AsyncIterator[MarketEvent]:
    """Write every event to `store`, yielding each one onward unchanged.

    A tee rather than a `store.write(event)` call in each consumer, so the
    recorded stream is *definitionally* identical to the emitted one -- same
    events, same order, nothing dropped by a caller's control flow. That is the
    property Phase 6 has to reproduce, and a file cannot show whether a caller
    honoured it.

    Yields:
        Exactly what `events` yielded, in the same order.
    """
    async for event in events:
        store.write(event)
        yield event
