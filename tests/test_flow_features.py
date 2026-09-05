"""Rolling-window features, and the clock they are allowed to use.

Every timestamp here is from 2020. That is the point: these tests only pass if
the window is measured against event time. Reach for `time.time()` anywhere in
`FlowFeatures` and every event falls outside the window, which is exactly what
would happen during a replay of recorded data.
"""

from decimal import Decimal

from tickforge.analytics import FlowFeatures
from tickforge.book import OrderBook
from tickforge.events import BookSnapshot, BookUpdate, Side, Trade

SECOND = 1_000_000_000
T0 = 1_580_000_000 * SECOND  # 2020-01-26, chosen to be long past
WINDOW = 60 * SECOND


def levels(pairs):
    return tuple((Decimal(p), Decimal(q)) for p, q in pairs)


def snapshot(at=T0, bids=(("100", "5"),), asks=(("101", "5"),)) -> BookSnapshot:
    return BookSnapshot("test", "TEST", at, at, 1, levels(bids), levels(asks))


def update(seq, at=T0, bids=(), asks=()) -> BookUpdate:
    return BookUpdate("test", "TEST", at, at, seq, seq, levels(bids), levels(asks))


def trade(price, quantity, aggressor, at=T0) -> Trade:
    return Trade(
        "test", "TEST", at, at, Decimal(price), Decimal(quantity), 1, aggressor
    )


class Pipeline:
    """A book and a FlowFeatures kept in step, as the real pipeline keeps them.

    `observe` must run after `apply`, since order flow reads the book's touch.
    Wrapping the pair is the cheapest way to stop a test getting that backwards
    and quietly measuring the previous state.
    """

    def __init__(self, window_ns=WINDOW) -> None:
        self.book = OrderBook("test", "TEST")
        self.flow = FlowFeatures(window_ns)

    def feed(self, *events):
        for event in events:
            if isinstance(event, BookSnapshot):
                self.book.load_snapshot(event)
            elif isinstance(event, BookUpdate):
                self.book.apply(event)
            self.flow.observe(event, self.book)
        return self.flow


def seeded(window_ns=WINDOW) -> Pipeline:
    p = Pipeline(window_ns)
    p.feed(snapshot())
    return p


# --- vwap -------------------------------------------------------------------


def test_vwap_weights_by_size_not_by_count() -> None:
    """A mean of prices would give 150. Nine of the ten units traded at 100."""
    p = seeded()
    p.feed(
        trade("100", "9", Side.BUY, T0 + SECOND),
        trade("200", "1", Side.BUY, T0 + 2 * SECOND),
    )
    assert p.flow.vwap == Decimal("110")


def test_vwap_is_none_before_anything_trades() -> None:
    """Zero trades is not a price of zero."""
    assert seeded().flow.vwap is None


# --- trade imbalance --------------------------------------------------------


def test_trade_imbalance_is_signed_share_of_volume() -> None:
    p = seeded()
    p.feed(
        trade("100", "3", Side.BUY, T0 + SECOND),
        trade("100", "1", Side.SELL, T0 + 2 * SECOND),
    )
    assert p.flow.trade_imbalance == Decimal("0.5")  # (3-1)/4


def test_trade_imbalance_weights_size_over_count() -> None:
    """Ten small sells do not outweigh one large buy. Counting trades rather
    than volume is the obvious wrong implementation and this catches it."""
    p = seeded()
    p.feed(trade("100", "100", Side.BUY, T0 + SECOND))
    p.feed(*[trade("100", "1", Side.SELL, T0 + 2 * SECOND) for _ in range(10)])
    assert p.flow.trade_imbalance > 0


# --- order flow imbalance ---------------------------------------------------


def test_size_added_at_the_bid_is_positive_flow() -> None:
    p = seeded()
    p.feed(update(2, T0 + SECOND, bids=(("100", "9"),)))
    assert p.flow.order_flow_imbalance == Decimal("4")  # 9 - 5


def test_size_added_at_the_ask_is_negative_flow() -> None:
    """Mirrored, and negative: offers arriving are selling pressure."""
    p = seeded()
    p.feed(update(2, T0 + SECOND, asks=(("101", "9"),)))
    assert p.flow.order_flow_imbalance == Decimal("-4")


def test_a_higher_bid_counts_its_whole_size() -> None:
    """A new best bid is new buying interest, not a size delta -- the old
    level is no longer the touch, so there is nothing to subtract from.
    """
    p = seeded()
    p.feed(update(2, T0 + SECOND, bids=(("100.5", "3"),)))
    assert p.flow.order_flow_imbalance == Decimal("3")


def test_the_bid_being_consumed_is_negative_flow() -> None:
    """Best bid deleted, so the touch drops to a worse price.

    Only the vanished size counts: -5. The 2 units at 99 are *not* added,
    even though they are now the touch, because they were already resting and
    did not move. Counting them would book pre-existing liquidity as newly
    arrived buying interest.
    """
    p = Pipeline()
    p.feed(snapshot(bids=(("100", "5"), ("99", "2"))))
    p.feed(update(2, T0 + SECOND, bids=(("100", "0"),)))
    assert p.flow.order_flow_imbalance == Decimal("-5")


def test_flow_accumulates_across_updates() -> None:
    p = seeded()
    p.feed(
        update(2, T0 + SECOND, bids=(("100", "7"),)),
        update(3, T0 + 2 * SECOND, bids=(("100", "9"),)),
    )
    assert p.flow.order_flow_imbalance == Decimal("4")  # +2 then +2


def test_no_updates_means_no_flow_not_unknown() -> None:
    """Zero, not None. An observed absence of flow is information."""
    assert seeded().flow.order_flow_imbalance == Decimal("0")


# --- realised volatility ----------------------------------------------------


def test_volatility_is_none_until_the_price_moves() -> None:
    p = seeded()
    p.feed(update(2, T0 + SECOND, bids=(("100", "9"),)))  # size only, same mid
    assert p.flow.realised_volatility is None


def test_a_moving_price_produces_volatility() -> None:
    p = seeded()
    p.feed(
        update(2, T0 + SECOND, bids=(("100.5", "5"),)),
        update(3, T0 + 2 * SECOND, bids=(("100", "5"),)),
    )
    assert p.flow.realised_volatility > 0


def test_a_bigger_move_produces_more_volatility() -> None:
    """Size of the move matters; direction must not, since returns are squared.

    Both updates delete the resting ask before adding the new one -- simply
    adding a level further out leaves the touch, and therefore the midprice,
    exactly where it was.
    """
    small = seeded()
    small.feed(update(2, T0 + SECOND, asks=(("101", "0"), ("101.5", "5"))))
    big = seeded()
    big.feed(update(2, T0 + SECOND, asks=(("101", "0"), ("110", "5"))))
    assert big.flow.realised_volatility > small.flow.realised_volatility


# --- the window -------------------------------------------------------------


def test_events_older_than_the_window_are_dropped() -> None:
    p = seeded()
    p.feed(trade("100", "1", Side.BUY, T0 + SECOND))
    assert p.flow.vwap == Decimal("100")

    # A trade 61s later carries the clock past the first one's expiry.
    p.feed(trade("200", "1", Side.BUY, T0 + 62 * SECOND))
    assert p.flow.vwap == Decimal("200")


def test_the_window_is_measured_in_event_time_not_wall_time() -> None:
    """The determinism test, and the reason every timestamp here is from 2020.

    A wall clock would put `now` five years past these events, placing all of
    them outside the window -- so VWAP would be None. That is precisely how a
    replay of recorded data fails while the live run it recorded looked fine.
    """
    p = seeded()
    p.feed(
        trade("100", "1", Side.BUY, T0 + SECOND),
        trade("102", "1", Side.BUY, T0 + 2 * SECOND),
    )
    assert p.flow.vwap == Decimal("101")


def test_the_clock_never_runs_backwards() -> None:
    """Trades are stamped at match time and depth frames at emission time, so
    they interleave slightly out of order. A receding cutoff would un-expire
    events and widen the window at random."""
    p = seeded()
    p.feed(trade("100", "1", Side.BUY, T0 + 100 * SECOND))
    p.feed(trade("200", "1", Side.BUY, T0 + 99 * SECOND))  # earlier than the last

    # Both are inside a 60s window ending at the later timestamp.
    assert p.flow.vwap == Decimal("150")

    p.feed(trade("300", "1", Side.BUY, T0 + 161 * SECOND))
    assert p.flow.vwap == Decimal("300")  # both earlier ones expired


# --- resynchronisation ------------------------------------------------------


def test_a_snapshot_makes_flow_unknown_not_zero() -> None:
    """The distinction the whole resync branch turns on.

    Clearing the window leaves zero, and zero reads as "no net flow" -- a calm
    number produced for a full window right after the one event saying the
    market was probably busy. Unknown has to look unknown.
    """
    p = seeded()
    p.feed(update(2, T0 + SECOND, bids=(("100", "9"),)))
    assert p.flow.order_flow_imbalance == Decimal("4")

    p.feed(snapshot(at=T0 + 2 * SECOND))
    assert p.flow.order_flow_imbalance is None
    assert p.flow.realised_volatility is None


def test_flow_becomes_known_again_once_the_window_clears_the_gap() -> None:
    """Once no part of the window predates the resync, the sum is honest."""
    p = seeded()
    p.feed(update(2, T0 + SECOND, bids=(("100", "9"),)))
    p.feed(snapshot(at=T0 + 2 * SECOND))
    assert p.flow.order_flow_imbalance is None

    # Sequence 2, not 3: the fresh snapshot reset the book to sequence 1, so
    # anything further ahead is a gap and would invalidate it.
    p.feed(update(2, T0 + 63 * SECOND, bids=(("100", "8"),)))
    assert p.flow.order_flow_imbalance == Decimal("3")  # 8 - 5, post-gap only


def test_a_first_snapshot_is_not_a_resync() -> None:
    """Nothing was missed -- observation starts here. Reporting unknown for a
    whole window at every startup would make the feature useless."""
    p = Pipeline()
    p.feed(snapshot())
    assert p.flow.order_flow_imbalance == Decimal("0")


def test_a_resync_does_not_make_trade_features_unknown() -> None:
    """Trades are self-contained, so a gap in book updates says nothing about
    them. Only the two continuity-dependent features go dark."""
    p = seeded()
    p.feed(trade("100", "1", Side.BUY, T0 + SECOND))
    p.feed(snapshot(at=T0 + 2 * SECOND))
    assert p.flow.vwap == Decimal("100")
    assert p.flow.trade_imbalance == Decimal("1")


def test_a_snapshot_keeps_the_trades() -> None:
    """Each trade is self-contained. The ones already recorded really happened,
    whatever the book was doing."""
    p = seeded()
    p.feed(trade("100", "1", Side.BUY, T0 + SECOND))
    p.feed(snapshot(at=T0 + 2 * SECOND))
    assert p.flow.vwap == Decimal("100")


def test_flow_resumes_from_the_new_snapshot() -> None:
    """The touch is re-seeded from the fresh book, so the first update after a
    resync produces a contribution rather than being silently skipped.

    Reads through the private deque because the property reports None until
    the window clears the gap -- this checks the measurement happened, which
    is a separate question from whether it is safe to report yet.
    """
    p = seeded()
    p.feed(snapshot(at=T0 + SECOND, bids=(("100", "5"),)))
    p.feed(update(2, T0 + 2 * SECOND, bids=(("100", "8"),)))
    assert [contribution for _, contribution in p.flow._ofi] == [Decimal("3")]
