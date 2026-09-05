"""Microstructure features are correct, and correct in the right direction.

Several of these check a *direction* rather than a value. That is deliberate:
microprice and imbalance both have a plausible wrong implementation that still
returns a number between the bid and the ask, so an equality assertion written
from the same formula being tested would pass against the bug.
"""

from decimal import Decimal

import pytest

from tickforge.analytics import (
    FlowFeatures,
    feature_snapshot,
    imbalance,
    market_depth,
    microprice,
    midprice,
    spread,
)
from tickforge.book import BookInvalidError, OrderBook
from tickforge.events import BookSnapshot, BookUpdate

BIDS = (("100", "2"), ("99", "6"), ("98", "8"))
ASKS = (("101", "6"), ("102", "6"), ("103", "4"))
"""Cumulative sizes are 2/8/16 and 6/12/16, chosen so imbalance is exact at
every depth and different at each -- repeating decimals would force every
assertion to be written with the operation it is testing."""


def levels(pairs):
    return tuple((Decimal(p), Decimal(q)) for p, q in pairs)


def book(bids=BIDS, asks=ASKS) -> OrderBook:
    b = OrderBook("test", "TEST")
    b.load_snapshot(BookSnapshot("test", "TEST", 0, 0, 1, levels(bids), levels(asks)))
    return b


def invalid_book() -> OrderBook:
    """Seeded, then sent an update far past its sequence, so it goes INVALID."""
    b = book()
    b.apply(BookUpdate("test", "TEST", 0, 0, 500, 510, (), ()))
    return b


# --- spread and midprice ----------------------------------------------------


def test_spread_is_ask_minus_bid() -> None:
    assert spread(book()) == Decimal("1")


def test_midprice_sits_halfway() -> None:
    assert midprice(book()) == Decimal("100.5")


def test_one_sided_book_has_no_spread_or_midprice() -> None:
    """A market with no offers is a real state, not an error."""
    assert spread(book(asks=())) is None
    assert midprice(book(asks=())) is None
    assert spread(book(bids=())) is None
    assert midprice(book(bids=())) is None


# --- microprice -------------------------------------------------------------


def test_microprice_leans_toward_the_lighter_side() -> None:
    """The test the crossed weighting exists for.

    Six units offered against two bid: the ask is the harder wall to break,
    so fair value sits below the midpoint. Pairing each price with its own
    quantity returns 100.75 here -- still between bid and ask, still
    plausible, and on the wrong side of the midpoint.
    """
    assert microprice(book()) == Decimal("100.25")
    assert microprice(book()) < midprice(book())


def test_microprice_leans_the_other_way_when_bids_are_heavy() -> None:
    heavy_bid = book(bids=(("100", "6"),), asks=(("101", "2"),))
    assert microprice(heavy_bid) > midprice(heavy_bid)


def test_balanced_book_gives_exactly_the_midprice() -> None:
    """Also the reason no other microprice test may use equal sizes: both the
    correct and the crossed-wrong formula agree here."""
    balanced = book(bids=(("100", "5"),), asks=(("101", "5"),))
    assert microprice(balanced) == midprice(balanced)


def test_microprice_is_midprice_shifted_by_l1_imbalance() -> None:
    """An identity, not a coincidence:

        microprice == midprice + (spread / 2) * imbalance(1)

    Microprice is L1 imbalance expressed in price rather than as a ratio.
    """
    b = book()
    shifted = midprice(b) + (spread(b) / 2) * imbalance(b, 1)
    assert microprice(b) == shifted


def test_microprice_needs_both_sides() -> None:
    assert microprice(book(asks=())) is None
    assert microprice(book(bids=())) is None


# --- market depth -----------------------------------------------------------


def test_depth_counts_only_the_levels_asked_for() -> None:
    """depth=1 summing the whole book would go unnoticed on a book whose
    deeper levels happen to be small."""
    assert market_depth(book(), 1) == (Decimal("2"), Decimal("6"))
    assert market_depth(book(), 2) == (Decimal("8"), Decimal("12"))
    assert market_depth(book(), 3) == (Decimal("16"), Decimal("16"))


def test_depth_beyond_the_book_returns_what_exists() -> None:
    """Asking for more levels than the venue sent is normal, not an error."""
    assert market_depth(book(), 50) == market_depth(book(), 3)


def test_empty_side_has_zero_depth_as_a_decimal() -> None:
    """Decimal(0), not int 0 -- the annotation says tuple[Decimal, Decimal],
    and a bare `sum()` over an empty side would quietly return an int."""
    bid_total, ask_total = market_depth(book(asks=()), 3)
    assert ask_total == 0
    assert isinstance(ask_total, Decimal)
    assert isinstance(bid_total, Decimal)


# --- imbalance --------------------------------------------------------------


def test_imbalance_is_depth_normalised() -> None:
    assert imbalance(book(), 1) == Decimal("-0.5")
    assert imbalance(book(), 2) == Decimal("-0.2")
    assert imbalance(book(), 3) == Decimal("0")


def test_swapping_the_sides_flips_the_sign() -> None:
    """Catches a subtraction written the wrong way round, which a
    single-book assertion cannot."""
    mirrored = book(bids=(("100", "6"),), asks=(("101", "2"),))
    normal = book(bids=(("100", "2"),), asks=(("101", "6"),))
    assert imbalance(mirrored, 1) == -imbalance(normal, 1)


def test_one_sided_book_saturates_rather_than_failing() -> None:
    """All buyers and no sellers is +1. That is the honest answer, not None."""
    assert imbalance(book(asks=()), 3) == Decimal("1")
    assert imbalance(book(bids=()), 3) == Decimal("-1")


def test_empty_book_has_no_imbalance() -> None:
    """Nothing resting on either side: the ratio is undefined, not zero.
    Returning zero would read downstream as 'perfectly balanced'."""
    assert imbalance(book(bids=(), asks=()), 3) is None


# --- failing closed ---------------------------------------------------------


# --- the combined snapshot --------------------------------------------------

WINDOW = 60 * 1_000_000_000


def test_snapshot_agrees_with_the_individual_functions() -> None:
    """One row must not be a second, drifting implementation of the features."""
    b = book()
    snap = feature_snapshot(b, FlowFeatures(WINDOW), 12345)

    assert snap.timestamp_ns == 12345
    assert snap.best_bid == b.best_bid
    assert snap.spread == spread(b)
    assert snap.midprice == midprice(b)
    assert snap.microprice == microprice(b)
    assert snap.imbalance_1 == imbalance(b, 1)
    assert snap.imbalance_5 == imbalance(b, 5)
    assert (snap.bid_depth_10, snap.ask_depth_10) == market_depth(b, 10)


def test_snapshot_carries_undefined_features_through_as_none() -> None:
    """A one-sided book has no midprice and a fresh window no VWAP -- but a
    one-sided book *does* have an imbalance, and it saturates rather than
    going unknown. The snapshot must not flatten that distinction."""
    snap = feature_snapshot(book(asks=()), FlowFeatures(WINDOW), 1)

    assert snap.midprice is None
    assert snap.microprice is None
    assert snap.spread is None
    assert snap.vwap is None
    assert snap.realised_volatility is None
    assert snap.imbalance_10 == Decimal("1")
    assert snap.order_flow_imbalance == Decimal("0")


def test_snapshot_refuses_an_invalid_book() -> None:
    """A feature row from a corrupt book is the plausible-wrong-number failure
    the whole project is arranged around. It must not be storable."""
    with pytest.raises(BookInvalidError):
        feature_snapshot(invalid_book(), FlowFeatures(WINDOW), 1)


# --- failing closed ---------------------------------------------------------


def test_every_feature_refuses_an_invalid_book() -> None:
    """Analytics inherit the book's fail-closed reads rather than reimplementing
    them. A feature computed from a known-corrupt book is exactly the
    plausible-wrong-number failure the project exists to avoid.
    """
    b = invalid_book()
    for feature in (spread, midprice, microprice):
        with pytest.raises(BookInvalidError):
            feature(b)
    with pytest.raises(BookInvalidError):
        market_depth(b, 5)
    with pytest.raises(BookInvalidError):
        imbalance(b, 5)
