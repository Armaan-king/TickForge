"""Print a capture, and dump it to CSV you can open in Excel.

Parquet is binary, so a capture cannot be read with a text editor. This turns
one into something you can look at.

    uv run python scripts/show_capture.py                 today, BTCUSDT
    uv run python scripts/show_capture.py 2026-09-07      a specific day
    uv run python scripts/show_capture.py 2026-09-07 csv  also write CSVs
"""

import datetime as dt
import sys
from pathlib import Path

import polars as pl


def capture_root() -> Path:
    """Where captures live: `runs/` if present, else `data/`.

    `runs/` is a junction to a folder outside OneDrive, used for long captures
    because hours of Parquet writes churn sync. A fresh clone has no such
    junction, so `data/` -- what the CLI writes by default -- is the fallback.
    """
    return Path("runs" if Path("runs").is_dir() else "data") / "binance" / "BTCUSDT"


ROOT = capture_root()
STREAMS = ("snapshots", "trades", "book_updates", "features")


def load(date: str, stream: str) -> pl.DataFrame | None:
    files = sorted(ROOT.joinpath(date).glob(f"{stream}-*.parquet"))
    return pl.read_parquet(files) if files else None


def flatten(frame: pl.DataFrame) -> pl.DataFrame:
    """One row per price level, for a stream that nests bids and asks.

    CSV has no nested types, so the levels have to come out somehow. Long
    format -- a `side` column rather than one row per event -- because
    exploding both sides at once would be a cartesian product, and because it
    is the shape a spreadsheet can actually filter and sort.
    """
    if "bids" not in frame.columns:
        return frame

    keys = [c for c in frame.columns if c not in ("bids", "asks")]
    sides = [
        frame.select(*keys, pl.lit(side).alias("side"), pl.col(column).alias("level"))
        .explode("level")
        .drop_nulls("level")
        .unnest("level")
        for side, column in (("bid", "bids"), ("ask", "asks"))
    ]
    return pl.concat(sides).sort("capture_seq", "side", "price", descending=[False, False, True])


def main() -> None:
    date = sys.argv[1] if len(sys.argv) > 1 else dt.datetime.now(dt.UTC).strftime("%Y-%m-%d")
    to_csv = len(sys.argv) > 2 and sys.argv[2] == "csv"

    if not ROOT.joinpath(date).is_dir():
        print(f"No capture at {ROOT / date}")
        print("Record one:  uv run python -m tickforge BTCUSDT 60 data")
        return

    print(f"\n{'=' * 70}\n{ROOT / date}\n{'=' * 70}")

    for stream in STREAMS:
        frame = load(date, stream)
        if frame is None:
            print(f"\n--- {stream}: nothing recorded")
            continue

        print(f"\n--- {stream}: {frame.height} rows, {len(frame.columns)} columns")
        print(frame.head(5))

        if to_csv:
            # Written beside the Parquet rather than into the repo root: only
            # data/ is gitignored, so market data cannot end up committed.
            out = ROOT / date / f"{stream}.csv"
            flat = flatten(frame)
            # Decimals become strings so Excel cannot round them into floats,
            # which is the whole reason the pipeline avoids floats to begin with.
            flat.with_columns(
                pl.col(name).cast(pl.Utf8)
                for name, dtype in zip(flat.columns, flat.dtypes)
                if dtype.base_type() == pl.Decimal
            ).write_csv(out)
            print(f"    -> {out}  ({flat.height} rows)")

    updates = load(date, "book_updates")
    trades = load(date, "trades")
    if updates is not None:
        ordered = updates.sort("first_seq")
        gaps = ordered.with_columns(
            (pl.col("first_seq") - pl.col("last_seq").shift(1) - 1).alias("gap")
        ).filter(pl.col("gap") != 0)
        print(f"\n{'=' * 70}")
        print(f"book updates : {ordered.height}")
        print(f"sequence gaps: {gaps.height}   (0 means the book saw every update)")
        print(
            f"covering     : {ordered['last_seq'][-1] - ordered['first_seq'][0]:,} "
            f"exchange sequence numbers"
        )
    if trades is not None:
        by_side = trades.group_by("aggressor").agg(
            pl.len().alias("count"), pl.col("quantity").sum().alias("volume")
        )
        print(f"\ntrades by aggressor:\n{by_side}")


if __name__ == "__main__":
    main()
