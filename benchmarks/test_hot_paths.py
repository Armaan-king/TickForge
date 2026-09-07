"""Per-operation timings for the hot paths, against a realistic book.

Not collected by the normal suite -- `testpaths` in pyproject.toml points at
`tests/`. Run explicitly::

    uv run pytest benchmarks/ --benchmark-columns=median,ops

Nothing here asserts a threshold. A benchmark that fails on a busy laptop
teaches nothing and gets deleted; these exist to be read, and to be re-run
after a change so the difference is measured rather than assumed.

The book is seeded with 1000 levels a side because that is what Binance's
snapshot endpoint returns at the depth this project requests. Benchmarking a
three-level book would measure nothing that happens in production.
"""

from decimal import Decimal

import pytest

from tickforge.adapters.binance import parse_depth_update, parse_stream_frame
from tickforge.analytics import (
    FlowFeatures,
    feature_snapshot,
    imbalance,
    market_depth,
    microprice,
)
from tickforge.book import OrderBook
from tickforge.events import BookSnapshot, BookUpdate

DEPTH = 1000
WINDOW_NS = 60 * 1_000_000_000


def _side(start: int, step: int) -> tuple[tuple[Decimal, Decimal], ...]:
    return tuple(
        (Decimal(start + step * n) / 100, Decimal(n % 17 + 1)) for n in range(DEPTH)
    )


@pytest.fixture
def book() -> OrderBook:
    """A 1000x1000 book, the shape a live snapshot actually produces."""
    b = OrderBook("binance", "BTCUSDT")
    b.load_snapshot(
        BookSnapshot("binance", "BTCUSDT", 0, 0, 1, _side(7938600, -1), _side(7938601, 1))
    )
    return b


@pytest.fixture
def flow(book) -> FlowFeatures:
    f = FlowFeatures(WINDOW_NS)
    f.observe(BookSnapshot("binance", "BTCUSDT", 0, 0, 1, (), ()), book)
    return f


UPDATE = BookUpdate(
    "binance", "BTCUSDT", 1, 1, 2, 2,
    tuple((Decimal(79386) - Decimal(n) / 100, Decimal(n + 1)) for n in range(60)),
    tuple((Decimal(79387) + Decimal(n) / 100, Decimal(n + 1)) for n in range(60)),
)
"""120 level changes -- the low end of Binance's 57-782 per frame."""

DEPTH_FRAME = (
    '{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":1788376136014,'
    '"s":"BTCUSDT","U":99588317538,"u":99588317663,'
    '"b":[["77381.36000000","1.29993000"],["77381.35000000","0.00084000"]],'
    '"a":[["77381.37000000","3.16444000"]]}}'
)


# --- parsing ----------------------------------------------------------------


def test_parse_stream_frame(benchmark) -> None:
    benchmark(parse_stream_frame, DEPTH_FRAME, 1)


def test_parse_depth_update(benchmark) -> None:
    """The unwrapped form, to separate envelope cost from body cost."""
    import json

    body = json.dumps(json.loads(DEPTH_FRAME)["data"])
    benchmark(parse_depth_update, body, 1)


# --- book -------------------------------------------------------------------


def test_apply_update(book, benchmark) -> None:
    """120 level changes into a 1000-level book, sequencing checked."""
    seq = iter(range(2, 10_000_000))

    def apply_next():
        n = next(seq)
        book.apply(
            BookUpdate("binance", "BTCUSDT", 1, 1, n, n, UPDATE.bids, UPDATE.asks)
        )

    benchmark(apply_next)


def test_best_bid(book, benchmark) -> None:
    """O(n) max over 1000 keys, and called several times per feature row."""
    benchmark(lambda: book.best_bid)


def test_top_bids_10(book, benchmark) -> None:
    """heapq.nlargest over 1000 keys. The suspected cost in feature_snapshot,
    which reaches it roughly ten times per row."""
    benchmark(book.top_bids, 10)


# --- analytics --------------------------------------------------------------


def test_microprice(book, benchmark) -> None:
    benchmark(microprice, book)


def test_imbalance_10(book, benchmark) -> None:
    benchmark(imbalance, book, 10)


def test_market_depth_10(book, benchmark) -> None:
    benchmark(market_depth, book, 10)


def test_feature_snapshot(book, flow, benchmark) -> None:
    """All thirteen features at once -- the dominant per-update cost measured
    by `python -m tickforge bench`."""
    benchmark(feature_snapshot, book, flow, 1)
