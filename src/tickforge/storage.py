"""Parquet persistence for the normalized event stream.

Records raw events, not derived state. Features can always be recomputed from
raw events; raw events cannot be recovered from features. That asymmetry is
what makes history re-derivable when book or analytics logic changes -- see
docs/knowledge/architecture.md -- and it is what Phase 6 replays.

Design and rejected alternatives:
docs/superpowers/specs/2026-09-05-parquet-storage-design.md
"""

import datetime as dt
import time
from collections.abc import AsyncIterator
from dataclasses import fields
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from tickforge.analytics import FeatureSnapshot
from tickforge.events import BookSnapshot, BookUpdate, MarketEvent, PriceLevel, Trade

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

# Repeats the directory path on every row, but Parquet dictionary-encodes a
# constant string column to near nothing, and it keeps a file self-describing
# if it is ever moved out of its partition.
_IDENTITY = [("exchange", pa.string()), ("symbol", pa.string())]

# int64 rather than pa.timestamp('ns'): the events already carry integers, and
# the timestamp type adds timezone semantics and a lossy conversion.
_TIMES = [("timestamp_ns", pa.int64()), ("received_ns", pa.int64())]

# The ONLY thing replay may order by. Neither timestamp can be: time.time_ns()
# resolves to ~0.6ms here so ~3% of events share a received_ns, and the
# exchange clock leads this machine by ~187ms so an event's timestamp can
# precede its own arrival. Distinct from the venue's first_seq/last_seq.
_ORDER = [("capture_seq", pa.int64())]

TRADE_SCHEMA = pa.schema(
    _IDENTITY
    + _TIMES
    + _ORDER
    + [
        ("trade_id", pa.int64()),
        ("price", PRICE),
        ("quantity", PRICE),
        # A StrEnum member *is* its string, so Side needs no conversion here
        # or on the way back. That is what the type was chosen for.
        ("aggressor", pa.string()),
    ]
)

UPDATE_SCHEMA = pa.schema(
    _IDENTITY
    + _TIMES
    + _ORDER
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
    + _ORDER
    + [
        # No range: a snapshot is the whole book as of one sequence number.
        # Sharing the update schema would mean nullable range columns and a
        # row whose meaning depends on which of them happen to be null.
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


def _level_structs(levels: tuple[PriceLevel, ...]) -> list[dict[str, Decimal]]:
    """Turn ``(price, quantity)`` pairs into the structs LEVELS expects.

    Named for its direction, because `binance._levels` does the opposite --
    wire strings into memory, where this takes memory onto disk.
    """
    return [{"price": price, "quantity": quantity} for price, quantity in levels]


FEATURE_SCHEMA = pa.schema(
    _IDENTITY
    + [("timestamp_ns", pa.int64())]
    + [
        (name, PRICE)
        for name in (
            "best_bid",
            "best_ask",
            "spread",
            "midprice",
            "microprice",
            "imbalance_1",
            "imbalance_5",
            "imbalance_10",
            "bid_depth_10",
            "ask_depth_10",
            "vwap",
            "trade_imbalance",
            "order_flow_imbalance",
            "realised_volatility",
        )
    ]
)
"""Derived features. No `received_ns`: a feature was never received.

Every column is nullable, and the nulls carry meaning -- a one-sided book has
no midprice, an empty window no VWAP, a window spanning a resync no order
flow. Reading a null as zero would turn "undefined" into "balanced".
"""

SCALE = Decimal("1e-18")
"""What derived values are rounded to before storage.

Dividing Decimals yields 28 *significant* digits -- an imbalance of
``(12-14)/26`` has 29 decimal places -- and pyarrow refuses to rescale into
decimal128(38,18) rather than truncate silently, which is the behaviour you
want. So the rounding is done here, explicitly.

Lossy, and acceptable only for *derived* values: features can always be
recomputed from the raw events, which are stored exactly. The same rounding
applied to a traded price would be unrecoverable, which is why raw events
never pass through here.
"""

STREAMS = {
    "trades": TRADE_SCHEMA,
    "book_updates": UPDATE_SCHEMA,
    "snapshots": SNAPSHOT_SCHEMA,
    "features": FEATURE_SCHEMA,
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

    Each store instance is one *session*, and its files carry a session stamp:
    ``trades-1788682769.parquet``. Parquet writers open for writing, not
    appending, so a fixed filename meant a second capture on the same day
    silently replaced the first. Readers treat a directory as one dataset, so
    nothing downstream notices the difference.

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
        session: int | None = None,
        roll_rows: int | None = None,
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
            session: Stamp distinguishing this capture's files from an earlier
                one in the same partition. Defaults to now. Wall clock, but it
                only names files -- it never reaches a stored value.
            roll_rows: Close the current files and start new ones after this
                many rows. Flushing writes row groups but not the footer, and a
                file without a footer cannot be read at all -- so a capture
                killed mid-run loses everything, not just its tail. Measured
                the hard way: a machine sleeping through an overnight run left
                12.5 MB of real market data unreadable.

                Rows rather than elapsed time, so no clock is involved and
                replaying the same events produces identical file boundaries.
                None keeps one file per session, which is right for short runs.
        """
        self._root = Path(root)
        self._exchange = exchange
        self._symbol = symbol
        self._batch_rows = batch_rows
        self._batch_ns = batch_ns
        self._roll_rows = roll_rows
        self._session = time.time_ns() if session is None else session
        self._date: str | None = None
        self._writers: dict[str, pq.ParquetWriter] = {}
        self._buffers: dict[str, list[dict]] = {name: [] for name in STREAMS}
        self._last_flush_ns: int | None = None
        # Dense across all three raw streams. Replay's ordering key, see _ORDER.
        self._capture_seq = 0
        self._rows_since_roll = 0

    def __enter__(self) -> "EventStore":
        return self

    def __exit__(self, *_) -> None:
        self.close()

    def write(self, event: MarketEvent) -> None:
        """Buffer one raw event, rolling the partition and flushing as needed."""
        stream, row = self._row(event)
        # After _row, so a rejected event consumes no number.
        row["capture_seq"] = self._capture_seq
        self._capture_seq += 1
        self._append(stream, row, event.timestamp_ns)

    def write_features(self, features: FeatureSnapshot) -> None:
        """Buffer one computed feature row.

        Separate from `write` because a `FeatureSnapshot` is not a market
        event: `record` tees the raw stream and must not carry derived values
        into what replay is meant to reproduce. Only the component that
        computed the features has them, so this is called directly.
        """
        self._append("features", self._feature_row(features), features.timestamp_ns)

    def _append(self, stream: str, row: dict, timestamp_ns: int) -> None:
        """Roll the partition if the day changed, buffer, flush if due."""
        date = _date_of(timestamp_ns)
        if date != self._date:
            self._roll(date)
        if self._last_flush_ns is None:
            self._last_flush_ns = timestamp_ns

        self._buffers[stream].append(row)
        self._rows_since_roll += 1

        if self._due(timestamp_ns):
            self.flush()
            self._last_flush_ns = timestamp_ns

        if self._roll_rows and self._rows_since_roll >= self._roll_rows:
            self._new_session()

    def _new_session(self) -> None:
        """Close the current files so they become readable, then start fresh.

        The stamp is incremented rather than re-read from the clock.
        `time.time_ns()` resolves to ~0.6 ms on Windows and rolls can happen
        microseconds apart, so re-reading it hands consecutive chunks the same
        stamp -- and the second silently overwrites the first. The same clock
        granularity that made `capture_seq` necessary, in a new place.

        `capture_seq` deliberately does not reset: it stays dense across the
        whole capture, so ordering within and between rolled files is
        unambiguous.
        """
        self.close()
        self._session += 1
        self._rows_since_roll = 0

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
        """Which stream this event belongs to, and its row.

        Raw events are written verbatim -- no rounding anywhere on this path.

        Raises:
            ValueError: The event is for a different instrument, or is a type
                this store has no schema for.
        """
        if (event.exchange, event.symbol) != (self._exchange, self._symbol):
            # Identity columns come from the store, so a mismatched event
            # would be silently relabelled and filed under the wrong symbol.
            raise ValueError(
                f"{event.exchange}:{event.symbol} event sent to a "
                f"{self._exchange}:{self._symbol} store"
            )

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
                "aggressor": event.aggressor,
            }
        if isinstance(event, BookSnapshot):
            return "snapshots", {
                **common,
                "last_seq": event.last_seq,
                "bids": _level_structs(event.bids),
                "asks": _level_structs(event.asks),
            }
        if isinstance(event, BookUpdate):
            return "book_updates", {
                **common,
                "first_seq": event.first_seq,
                "last_seq": event.last_seq,
                "bids": _level_structs(event.bids),
                "asks": _level_structs(event.asks),
            }
        # Explicit rather than a fallthrough, which would land a new event
        # type in book_updates and fail on a missing field instead.
        raise ValueError(f"no schema for {type(event).__name__}")

    def _feature_row(self, features: FeatureSnapshot) -> dict:
        """Round every derived value to the stored scale.

        Driven off the dataclass rather than a hand-written column list, so a
        new feature reaches storage by being added in one place.
        """
        row: dict = {
            "exchange": self._exchange,
            "symbol": self._symbol,
            "timestamp_ns": features.timestamp_ns,
        }
        for field in fields(features):
            if field.name == "timestamp_ns":
                continue
            value = getattr(features, field.name)
            row[field.name] = None if value is None else value.quantize(SCALE)
        return row

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
                directory / f"{name}-{self._session}.parquet", STREAMS[name]
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
