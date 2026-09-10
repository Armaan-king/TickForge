"""Read-only HTTP access to recorded captures, for downstream research.

A thin boundary over storage and replay, so a consumer can pull historical
market data without importing TickForge internals or rebuilding its Binance,
sequencing and replay logic.

**Market facts only.** Events go out exactly as recorded, in exactly the order
`replay.read_partition` produces. Nothing here infers, labels, aggregates,
windows or normalises -- an L2 quantity going to zero is reported as a level
with quantity zero, not as a CANCEL, because inferring intent from depth is a
modelling decision and belongs to the consumer.

Two access paths, for two different jobs:

- ``/events`` returns JSON pages, for browsing and small ranges.
- ``/export`` streams one merged Parquet file, for datasets.

The split exists because event sizes vary by ~750x. A trade is ~264 bytes of
JSON; a book update with 3,111 levels is 200 KB, and a 2,000-level snapshot is
150 KB. Measured on real captures -- see the byte budget in `read_range`.
"""

import io
import os
from collections.abc import Iterator
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from typing import Annotated

from tickforge.events import BookSnapshot, BookUpdate, MarketEvent, Trade
from tickforge.replay import event_from_row, session_files

EXCHANGE = "binance"
STREAMS = ("snapshots", "book_updates", "trades")
"""Raw streams only. `features` is derived output and is deliberately not
served here: a consumer that wants features recomputes them from the events,
which is what makes them reproducible."""

MAX_LIMIT = 5_000
DEFAULT_LIMIT = 500
BYTE_BUDGET = 4 * 1024 * 1024
"""Roughly how much JSON one page may carry before it is cut short.

A row limit alone cannot bound a response here. Measured on a real capture, a
trade is ~264 bytes and a book update's median is 8 KB with a 200 KB tail, so
500 events is anywhere between 130 KB and 100 MB depending on the mix. The
budget ends a page early and hands back a cursor.
"""

router = APIRouter(prefix="/research", tags=["research"])


def data_root() -> Path:
    """Where captures live.

    ``TICKFORGE_DATA_ROOT`` wins; otherwise ``runs/`` if it exists, else
    ``data/``. `runs/` is where long captures are kept, `data/` is what the CLI
    writes by default, and a fresh clone has only the latter.
    """
    configured = os.environ.get("TICKFORGE_DATA_ROOT")
    if configured:
        return Path(configured)
    return Path("runs" if Path("runs").is_dir() else "data")


def _partition(root: Path, symbol: str, date: str) -> Path:
    return root / EXCHANGE / symbol.upper() / date


def _column_range(path: Path, column: str) -> tuple[int, int] | None:
    """Min and max of one column, from the Parquet footer.

    Row-group statistics, so this never reads a data page. A 34-file partition
    catalogues in ~265 ms.
    """
    metadata = pq.ParquetFile(path).metadata
    if column not in metadata.schema.names:
        return None
    index = metadata.schema.names.index(column)
    low = high = None
    for group in range(metadata.num_row_groups):
        stats = metadata.row_group(group).column(index).statistics
        if stats is None:
            return None
        low = stats.min if low is None else min(low, stats.min)
        high = stats.max if high is None else max(high, stats.max)
    return None if low is None else (low, high)


def catalogue(root: Path, symbol: str, date: str | None = None) -> list[dict]:
    """What has been recorded, without reading any of it.

    Returns one entry per UTC date, each listing its capture sessions and, per
    stream, the row count and the `capture_seq` and `timestamp_ns` ranges the
    file covers. That is enough for a consumer to plan its downloads.
    """
    base = root / EXCHANGE / symbol.upper()
    if not base.is_dir():
        return []

    dates = [date] if date else sorted(p.name for p in base.iterdir() if p.is_dir())
    out = []
    for day in dates:
        directory = base / day
        if not directory.is_dir():
            continue
        sessions = []
        for session in sorted(session_files(directory)):
            streams = {}
            for stream, path in sorted(session_files(directory)[session].items()):
                metadata = pq.ParquetFile(path).metadata
                streams[stream] = {
                    "rows": metadata.num_rows,
                    "bytes": path.stat().st_size,
                    "capture_seq": _column_range(path, "capture_seq"),
                    "timestamp_ns": _column_range(path, "timestamp_ns"),
                }
            sessions.append(
                {
                    "session": session,
                    "events": sum(s["rows"] for s in streams.values()),
                    "streams": streams,
                }
            )
        if sessions:
            out.append(
                {
                    "date": day,
                    "sessions": sessions,
                    "events": sum(s["events"] for s in sessions),
                }
            )
    return out


def _filters(
    start_seq: int | None, end_seq: int | None, start_ns: int | None, end_ns: int | None
) -> list[tuple] | None:
    """Predicate pushdown for a filtered read.

    pyarrow uses row-group statistics to skip pages that cannot match, which
    measured 50x faster than reading a file and discarding rows.
    """
    clauses = []
    if start_seq is not None:
        clauses.append(("capture_seq", ">=", start_seq))
    if end_seq is not None:
        clauses.append(("capture_seq", "<=", end_seq))
    if start_ns is not None:
        clauses.append(("timestamp_ns", ">=", start_ns))
    if end_ns is not None:
        clauses.append(("timestamp_ns", "<=", end_ns))
    return clauses or None


def read_range(
    root: Path,
    symbol: str,
    date: str,
    session: int | None = None,
    start_seq: int | None = None,
    end_seq: int | None = None,
    start_ns: int | None = None,
    end_ns: int | None = None,
    after: tuple[int, int] | None = None,
    limit: int = DEFAULT_LIMIT,
    byte_budget: int = BYTE_BUDGET,
) -> tuple[list[tuple[int, MarketEvent]], tuple[int, int] | None]:
    """Ordered ``(session, event)`` pairs, and the cursor to continue from.

    Ordering is ``(session, capture_seq)`` -- identical to
    `replay.read_partition`, and for the same reason: `capture_seq` restarts at
    zero for every capture, so it cannot order two captures recorded on one
    day, and neither timestamp column can order anything because both have ties
    and they disagree with each other.

    Args:
        after: Resume strictly after this ``(session, capture_seq)``.
        byte_budget: Stop early once the page's approximate JSON size passes
            this, since 500 book updates and 500 trades differ by ~750x.

    Returns:
        ``(pairs, next_cursor)``. `next_cursor` is None when the range is
        exhausted.
    """
    directory = _partition(root, symbol, date)
    if not directory.is_dir():
        raise FileNotFoundError(f"no capture at {directory}")

    available = session_files(directory)
    wanted = [s for s in sorted(available) if session is None or s == session]

    collected: list[tuple[int, MarketEvent]] = []
    spent = 0
    for stamp in wanted:
        if after is not None and stamp < after[0]:
            continue
        floor = start_seq
        if after is not None and stamp == after[0]:
            resume = after[1] + 1
            floor = resume if floor is None else max(floor, resume)

        rows: list[tuple[int, str, dict]] = []
        for stream, path in available[stamp].items():
            table = pq.read_table(path, filters=_filters(floor, end_seq, start_ns, end_ns))
            for row in table.to_pylist():
                rows.append((row["capture_seq"], stream, row))
        rows.sort(key=lambda item: item[0])

        for seq, stream, row in rows:
            if len(collected) >= limit or spent >= byte_budget:
                return collected, (stamp, seq - 1) if collected else None
            collected.append((stamp, event_from_row(row, stream)))
            spent += _approx_size(row)

    return collected, None


def _approx_size(row: dict) -> int:
    """Roughly what this row costs as JSON, without serialising it twice.

    Levels dominate: each is two 18-decimal strings plus the struct around
    them, about 60 bytes. Everything else is a fixed overhead.
    """
    levels = len(row.get("bids") or ()) + len(row.get("asks") or ())
    return 200 + levels * 60


def as_json(session: int, event: MarketEvent) -> dict:
    """One event as it goes on the wire.

    Prices and quantities are strings. A JSON number becomes a float in every
    client, and the exactness the whole pipeline preserves would be discarded
    at the last step -- see `decisions.md` on Decimal.
    """
    body = {
        "type": (
            "snapshot" if isinstance(event, BookSnapshot)
            else "trade" if isinstance(event, Trade)
            else "book_update"
        ),
        "session": session,
        "exchange": event.exchange,
        "symbol": event.symbol,
        "timestamp_ns": event.timestamp_ns,
        "received_ns": event.received_ns,
    }
    if isinstance(event, Trade):
        body |= {
            "trade_id": event.trade_id,
            "price": str(event.price),
            "quantity": str(event.quantity),
            "aggressor": event.aggressor.value,
        }
        return body

    body["last_seq"] = event.last_seq
    if isinstance(event, BookUpdate):
        body["first_seq"] = event.first_seq
    body |= {
        "bids": [{"price": str(p), "quantity": str(q)} for p, q in event.bids],
        "asks": [{"price": str(p), "quantity": str(q)} for p, q in event.asks],
    }
    return body


def export_parquet(
    root: Path, symbol: str, date: str, stream: str, session: int | None = None
) -> Iterator[bytes]:
    """Every file for one stream and date, merged into one Parquet download.

    Written row group by row group, so peak memory stays around 0.2 MiB for a
    27 MiB dataset rather than the whole thing. Parquet rather than Arrow IPC
    because it is already the project's format and this needs no re-encoding
    decision -- and merged output measured *smaller* than the sources it came
    from, since 34 footers collapse into one.

    A ``session`` column is added, which the stored files do not carry because
    their filename holds it. The merged file has no filename per row, and
    `capture_seq` restarts at zero for every capture -- so without it a
    consumer who sorts by `capture_seq`, the obvious thing to do, silently
    interleaves two captures. Sort by ``(session, capture_seq)``.

    Yields:
        Chunks of a single valid Parquet file, ordered by `capture_seq` within
        each session and by session across them.
    """
    directory = _partition(root, symbol, date)
    if not directory.is_dir():
        raise FileNotFoundError(f"no capture at {directory}")

    available = session_files(directory)
    wanted = [
        (stamp, available[stamp][stream])
        for stamp in sorted(available)
        if (session is None or stamp == session) and stream in available[stamp]
    ]
    if not wanted:
        raise FileNotFoundError(f"no {stream} recorded for {symbol} on {date}")

    buffer = io.BytesIO()
    writer: pq.ParquetWriter | None = None
    for stamp, path in wanted:
        for batch in pq.ParquetFile(path).iter_batches(batch_size=8192):
            stamped = batch.append_column(
                "session", pa.array([stamp] * batch.num_rows, type=pa.int64())
            )
            if writer is None:
                writer = pq.ParquetWriter(buffer, stamped.schema, compression="zstd")
            writer.write_batch(stamped)
            if buffer.tell() > 1 << 20:
                yield buffer.getvalue()
                buffer.seek(0)
                buffer.truncate()
    if writer is not None:
        writer.close()  # writes the footer, without which nothing can read it
    if buffer.tell():
        yield buffer.getvalue()


# --- endpoints --------------------------------------------------------------


@router.get("/{symbol}/sessions")
async def get_sessions(symbol: str, date: str | None = None) -> dict:
    """What has been recorded for a symbol, and how much of it."""
    dates = catalogue(data_root(), symbol, date)
    return {
        "exchange": EXCHANGE,
        "symbol": symbol.upper(),
        "dates": dates,
        "events": sum(d["events"] for d in dates),
    }


@router.get("/{symbol}/events")
async def get_events(
    symbol: str,
    date: str,
    session: int | None = None,
    start_seq: int | None = None,
    end_seq: int | None = None,
    start_ns: int | None = None,
    end_ns: int | None = None,
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
) -> dict:
    """A page of recorded events, in replay order.

    `cursor` is opaque: it carries `(session, capture_seq)` because
    `capture_seq` restarts per capture and cannot order two of them alone.
    """
    after = _parse_cursor(cursor)
    try:
        pairs, nxt = read_range(
            data_root(), symbol, date, session,
            start_seq, end_seq, start_ns, end_ns, after, limit,
        )
    except FileNotFoundError as missing:
        raise HTTPException(status_code=404, detail=str(missing)) from None

    return {
        "exchange": EXCHANGE,
        "symbol": symbol.upper(),
        "date": date,
        "count": len(pairs),
        "next_cursor": None if nxt is None else f"{nxt[0]}:{nxt[1]}",
        "events": [as_json(stamp, event) for stamp, event in pairs],
    }


@router.get("/{symbol}/export")
async def get_export(
    symbol: str,
    date: str,
    stream: str,
    session: int | None = None,
) -> StreamingResponse:
    """One stream and date as a single merged Parquet download."""
    if stream not in STREAMS:
        raise HTTPException(
            status_code=400, detail=f"stream must be one of {', '.join(STREAMS)}"
        )
    root = data_root()
    try:
        chunks = export_parquet(root, symbol, date, stream, session)
        first = next(chunks)
    except FileNotFoundError as missing:
        raise HTTPException(status_code=404, detail=str(missing)) from None
    except StopIteration:
        raise HTTPException(status_code=404, detail="nothing recorded") from None

    name = f"{symbol.upper()}-{date}-{stream}.parquet"

    def body() -> Iterator[bytes]:
        yield first
        yield from chunks

    return StreamingResponse(
        body(),
        media_type="application/vnd.apache.parquet",
        headers={"Content-Disposition": f'attachment; filename="{name}"'},
    )


def _parse_cursor(cursor: str | None) -> tuple[int, int] | None:
    if cursor is None:
        return None
    try:
        stamp, seq = cursor.split(":")
        return int(stamp), int(seq)
    except ValueError:
        raise HTTPException(status_code=400, detail="malformed cursor") from None
