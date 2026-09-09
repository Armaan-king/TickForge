"""Summarise a long capture: the log, then the data it produced.

Written for reading the morning after an overnight run.

    uv run python scripts/run_report.py
    uv run python scripts/run_report.py runs 2026-09-09
"""

import datetime as dt
import re
import sys
from pathlib import Path

import polars as pl

HEARTBEAT = re.compile(
    r"^(?P<at>[\d-]+ [\d:]+)\s+mid (?P<mid>[\d.]+).*?"
    r"(?P<events>[\d,]+) events\s+(?P<trades>[\d,]+) trades\s+"
    r"(?P<snapshots>\d+) snapshot\(s\)\s+(?P<rejected>\d+) rejected"
)


def read_log(path: Path) -> None:
    if not path.exists():
        print(f"no log at {path}")
        return

    lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    beats = [m for m in (HEARTBEAT.match(line) for line in lines) if m]
    resyncs = [line for line in lines if "RESYNC" in line]
    rejects = [line for line in lines if "!!" in line]
    errors = [line for line in lines if "Traceback" in line or "Error" in line]

    print(f"=== {path}  ({len(lines)} lines) ===")
    if lines:
        print(f"first : {lines[0]}")
        print(f"last  : {lines[-1]}")

    if beats:
        first, last = beats[0], beats[-1]
        started = dt.datetime.strptime(first["at"], "%Y-%m-%d %H:%M:%S")
        ended = dt.datetime.strptime(last["at"], "%Y-%m-%d %H:%M:%S")
        mids = [float(b["mid"]) for b in beats]
        print(f"\nheartbeats : {len(beats)} over {ended - started}")
        print(f"events     : {last['events']}   trades: {last['trades']}")
        print(f"mid range  : {min(mids):,.2f} .. {max(mids):,.2f}  "
              f"({(max(mids) - min(mids)) / min(mids) * 100:.2f}% span)")

    # The three lines worth waking up to. Printed in full rather than counted,
    # because one resync tells you more than a hundred heartbeats.
    print(f"\nresyncs    : {len(resyncs)}")
    for line in resyncs[:20]:
        print(f"    {line}")
    print(f"rejected   : {len(rejects)}")
    for line in rejects[:20]:
        print(f"    {line}")
    if errors:
        print(f"errors     : {len(errors)}")
        for line in errors[:20]:
            print(f"    {line}")


def read_data(root: Path, dates: list[str]) -> None:
    """Report across every partition the run touched.

    An overnight run crosses midnight UTC, so it writes two date directories.
    Reading one would silently report on half the session.
    """
    dirs = [d for d in (root / "binance" / "BTCUSDT" / x for x in dates) if d.is_dir()]
    if not dirs:
        print(f"\nno capture under {root / 'binance' / 'BTCUSDT'}")
        return

    print(f"\n=== {len(dirs)} partition(s): {', '.join(d.name for d in dirs)} ===")
    total = 0
    for directory in dirs:
        for path in sorted(directory.glob("*.parquet")):
            total += path.stat().st_size
            print(f"    {directory.name}/{path.name:<44} {path.stat().st_size:>12,} bytes")
    print(f"    {'total':<56} {total:>12,} bytes  ({total / 1024 / 1024:.1f} MiB)")

    def read_all(stream: str) -> pl.DataFrame | None:
        """Every session's rows for one stream, skipping files still being written.

        A Parquet file has no footer until its writer closes, so a capture that
        is still running -- or one that was killed -- leaves a file nothing can
        read. Skipping it and saying so beats a traceback that looks like the
        data is corrupt.
        """
        frames = []
        for directory in dirs:
            for path in sorted(directory.glob(f"{stream}-*.parquet")):
                try:
                    frames.append(pl.read_parquet(path))
                except Exception:
                    print(f"    (skipped {path.name}: no footer, still open or killed)")
        return pl.concat(frames) if frames else None

    updates = read_all("book_updates")
    if updates is None:
        print("\nNo readable book updates yet. If the capture is still running,")
        print("wait for it to finish -- Parquet footers are written on close.")
        return
    ordered = updates.sort("first_seq")
    gaps = ordered.with_columns(
        (pl.col("first_seq") - pl.col("last_seq").shift(1) - 1).alias("gap")
    ).filter(pl.col("gap") != 0)

    span = ordered["last_seq"][-1] - ordered["first_seq"][0]
    print(f"\nbook updates  : {ordered.height:,}")
    print(f"sequence span : {span:,} exchange sequence numbers")
    print(f"sequence gaps : {gaps.height}")
    if gaps.height:
        # A gap here is expected wherever the log shows a resync: the feed
        # reconnects at whatever sequence the new snapshot carries.
        print(gaps.select("first_seq", "last_seq", "gap").head(10))

    trades = read_all("trades")
    if trades is not None:
        print(f"\ntrades        : {trades.height:,}")
        print(trades.group_by("aggressor").agg(
            pl.len().alias("count"), pl.col("quantity").sum().alias("volume")
        ))

    features = read_all("features")
    if features is not None:
        print(f"\nfeature rows  : {features.height:,}")
        print("nulls per column (a null is 'undefined', never zero):")
        nulls = features.null_count().transpose(include_header=True, column_names=["nulls"])
        print(nulls.filter(pl.col("nulls") > 0))


def main() -> None:
    # Polars draws tables with box characters, which Windows' cp1252 console
    # cannot encode. Without this the report crashes the moment it prints one.
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    # `runs/` is a junction to wherever long captures are kept; `data/` is
    # what the CLI writes by default. No absolute path, so this works on a
    # fresh clone rather than only on the machine it was written on.
    default = "runs" if Path("runs").is_dir() else "data"
    root = Path(sys.argv[1] if len(sys.argv) > 1 else default)
    if len(sys.argv) > 2:
        dates = sys.argv[2:]
    else:
        # Yesterday and today, because a night run spans both.
        today = dt.datetime.now(dt.UTC)
        dates = [
            (today - dt.timedelta(days=1)).strftime("%Y-%m-%d"),
            today.strftime("%Y-%m-%d"),
        ]

    # The launcher writes capture-<stamp>.log; older runs used overnight.log.
    logs = sorted(root.glob("capture-*.log")) + sorted(root.glob("overnight.log"))
    read_log(logs[-1] if logs else root / "overnight.log")
    read_data(root, dates)

    err = root / "overnight.err"
    if err.exists() and err.stat().st_size:
        print(f"\n=== stderr ({err}) ===")
        print(err.read_text(encoding="utf-8", errors="replace")[:4000])


if __name__ == "__main__":
    main()
