"""Order book maintains correct state and refuses to serve when it cannot."""

from decimal import Decimal

import pytest

from tickforge.book import ApplyResult, BookInvalidError, BookState, OrderBook
from tickforge.events import BookSnapshot, BookUpdate


def levels(pairs):
    return tuple((Decimal(p), Decimal(q)) for p, q in pairs)


def update(first_seq, last_seq, bids=(), asks=()):
    return BookUpdate(
        exchange="test",
        symbol="TEST",
        timestamp_ns=first_seq,
        received_ns=first_seq,
        first_seq=first_seq,
        last_seq=last_seq,
        bids=levels(bids),
        asks=levels(asks),
    )


def snapshot(last_seq, bids=(), asks=()):
    return BookSnapshot(
        exchange="test",
        symbol="TEST",
        timestamp_ns=last_seq,
        received_ns=last_seq,
        last_seq=last_seq,
        bids=levels(bids),
        asks=levels(asks),
    )


def seeded(last_seq=100, bids=(("100", "1"),), asks=(("101", "1"),)):
    """A book ready to receive updates, as it would be in production."""
    book = OrderBook("test", "TEST")
    book.load_snapshot(snapshot(last_seq, bids, asks))
    return book


# --- snapshot loading -------------------------------------------------------


def test_snapshot_seeds_the_book() -> None:
    book = seeded(100)
    assert book.state is BookState.SEEDED
    assert book.is_valid
    assert book.last_sequence == 100
    assert book.best_bid == Decimal("100")


def test_snapshot_replaces_rather_than_merges() -> None:
    """Merging would preserve levels the exchange no longer reports."""
    book = seeded(100, bids=(("100", "1"), ("99", "1")))
    book.load_snapshot(snapshot(200, bids=(("98", "5"),), asks=(("99", "1"),)))
    assert book._bids == {Decimal("98"): Decimal("5")}


def test_snapshot_for_wrong_instrument_is_rejected() -> None:
    book = OrderBook("test", "TEST")
    other = BookSnapshot("test", "OTHER", 1, 1, 1, (), ())
    with pytest.raises(ValueError, match="OTHER"):
        book.load_snapshot(other)


def test_crossed_snapshot_is_not_trusted() -> None:
    book = OrderBook("test", "TEST")
    assert book.load_snapshot(
        snapshot(100, bids=(("101", "1"),), asks=(("100", "1"),))
    ) is BookState.INVALID


# --- sequencing -------------------------------------------------------------


def test_updates_are_refused_before_a_snapshot() -> None:
    """A book built from diffs alone is incomplete, so it must not start."""
    book = OrderBook("test", "TEST")
    assert book.apply(update(1, 5, bids=[("100", "1")])) is ApplyResult.NOT_SYNCED
    assert book._bids == {}


def test_first_update_after_snapshot_may_span_the_boundary() -> None:
    """The snapshot lands partway through an update's range, so the first
    update starts *before* last_seq + 1 rather than exactly at it."""
    book = seeded(100)
    assert book.apply(update(95, 105, bids=[("99", "1")])) is ApplyResult.APPLIED
    assert book.state is BookState.SYNCED


def test_steady_state_requires_exact_contiguity() -> None:
    book = seeded(100)
    book.apply(update(95, 105))
    assert book.apply(update(107, 110)) is ApplyResult.GAP
    assert book.state is BookState.INVALID


def test_contiguous_updates_apply() -> None:
    book = seeded(100)
    book.apply(update(101, 105))
    assert book.apply(update(106, 109)) is ApplyResult.APPLIED


def test_update_entirely_before_the_snapshot_is_discarded() -> None:
    """Buffered updates predating the snapshot must be dropped, not applied."""
    book = seeded(100)
    assert book.apply(update(50, 60, bids=[("1", "1")])) is ApplyResult.DUPLICATE
    assert Decimal("1") not in book._bids


def test_gap_does_not_apply_the_update() -> None:
    """A book known to be wrong must not get more wrong."""
    book = seeded(100)
    book.apply(update(101, 105))
    book.apply(update(107, 110, bids=[("42", "1")]))
    assert Decimal("42") not in book._bids


def test_invalid_book_refuses_further_updates() -> None:
    book = seeded(100)
    book.apply(update(107, 110))
    assert book.apply(update(111, 112)) is ApplyResult.NOT_SYNCED


# --- level maintenance ------------------------------------------------------


def test_levels_are_inserted_updated_and_deleted() -> None:
    book = seeded(100, bids=(("100", "1"),), asks=())
    book.apply(update(101, 101, bids=[("99", "2"), ("98", "3")]))
    book.apply(update(102, 102, bids=[("99", "5"), ("98", "0")]))

    assert book._bids == {Decimal("100"): Decimal("1"), Decimal("99"): Decimal("5")}


def test_deleting_an_unknown_level_is_a_no_op() -> None:
    """Exchanges report removals for levels outside the tracked depth."""
    book = seeded(100)
    assert book.apply(update(101, 101, bids=[("50", "0")])) is ApplyResult.APPLIED


def test_best_prices_track_the_top_of_book() -> None:
    book = seeded(100, bids=(("100", "1"), ("99", "1")), asks=(("101", "1"), ("102", "1")))
    assert book.best_bid == Decimal("100")
    assert book.best_ask == Decimal("101")

    book.apply(update(101, 101, bids=[("100", "0")]))
    assert book.best_bid == Decimal("99")


def test_empty_side_gives_none_not_an_error() -> None:
    book = seeded(100, bids=(("100", "1"),), asks=())
    assert book.best_ask is None


# --- failing closed ---------------------------------------------------------


def test_crossed_book_is_detected_and_invalidates() -> None:
    book = seeded(100)
    assert book.apply(update(101, 101, asks=[("99", "1")])) is ApplyResult.CROSSED
    assert book.state is BookState.INVALID


def test_reads_fail_closed_when_invalid() -> None:
    """Ignoring the ApplyResult must not yield plausible wrong numbers."""
    book = seeded(100)
    book.apply(update(107, 110))

    with pytest.raises(BookInvalidError):
        _ = book.best_bid
    with pytest.raises(BookInvalidError):
        _ = book.best_ask


def test_unseeded_book_refuses_reads() -> None:
    with pytest.raises(BookInvalidError, match="EMPTY"):
        _ = OrderBook("test", "TEST").best_bid


# --- depth access -----------------------------------------------------------

DEEP = (
    ("99", "3"),
    ("101", "1"),
    ("97", "5"),
    ("100", "2"),
    ("98", "4"),
)
"""Deliberately unsorted. A dict preserves insertion order, so a method that
forgot to sort would still look right if the fixture arrived sorted."""


def test_top_bids_are_ordered_best_first() -> None:
    book = seeded(100, bids=DEEP, asks=(("200", "1"),))

    top = book.top_bids(3)

    assert [price for price, _ in top] == [Decimal("101"), Decimal("100"), Decimal("99")]
    assert top[0] == (book.best_bid, Decimal("1"))


def test_top_asks_are_ordered_best_first() -> None:
    """The mirror matters: one side returned in the wrong direction is
    invisible until an imbalance number comes out inverted."""
    book = seeded(100, bids=(("1", "1"),), asks=DEEP)

    top = book.top_asks(3)

    assert [price for price, _ in top] == [Decimal("97"), Decimal("98"), Decimal("99")]
    assert top[0] == (book.best_ask, Decimal("5"))


def test_quantities_travel_with_their_price() -> None:
    """An off-by-one between the price list and the quantity lookup would
    still produce a plausibly ordered book."""
    book = seeded(100, bids=DEEP, asks=(("200", "1"),))

    assert book.top_bids(5) == (
        (Decimal("101"), Decimal("1")),
        (Decimal("100"), Decimal("2")),
        (Decimal("99"), Decimal("3")),
        (Decimal("98"), Decimal("4")),
        (Decimal("97"), Decimal("5")),
    )


def test_asking_for_more_levels_than_exist() -> None:
    """A thin book returns what it has rather than padding or raising."""
    book = seeded(100, bids=(("100", "1"), ("99", "1")))

    assert len(book.top_bids(10)) == 2


def test_depth_reads_fail_closed() -> None:
    """The reason depth lives on the book and not in the analytics layer:
    serving levels from an INVALID book is the unrecoverable failure."""
    book = seeded(100, bids=DEEP, asks=(("200", "1"),))
    book.apply(update(107, 110))

    with pytest.raises(BookInvalidError):
        book.top_bids(5)
    with pytest.raises(BookInvalidError):
        book.top_asks(5)
