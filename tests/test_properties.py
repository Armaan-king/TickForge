"""Invariants that must hold for *any* event sequence, not just chosen ones.

The example-based tests elsewhere assert what happens for cases someone
thought of. These assert what must never happen at all, against sequences
Hypothesis generates and then shrinks to the smallest input that still breaks
them.

This is the layer that matters most to anything built on top of TickForge: a
generative model trained on recorded state needs the guarantee that no
reachable sequence produces a book which looks valid and is not.
"""

import tempfile
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from tickforge.analytics import imbalance, market_depth, microprice, midprice, spread
from tickforge.book import ApplyResult, BookInvalidError, BookState, OrderBook
from tickforge.events import BookSnapshot, BookUpdate, Side, Trade
from tickforge.replay import read_partition
from tickforge.storage import EventStore

# Two decimal places and a bounded range: wide enough to cross, collide and
# delete, narrow enough that Hypothesis explores those cases instead of
# wandering through unreachable magnitudes.
prices = st.decimals(min_value=Decimal("1"), max_value=Decimal("200"), places=2)
quantities = st.decimals(min_value=Decimal("0"), max_value=Decimal("50"), places=2)
levels = st.lists(st.tuples(prices, quantities), max_size=6).map(tuple)


def book_from(bids, asks) -> OrderBook:
    book = OrderBook("test", "TEST")
    book.load_snapshot(BookSnapshot("test", "TEST", 0, 0, 1, bids, asks))
    return book


@st.composite
def two_sided_books(draw) -> OrderBook:
    """A seeded, uncrossed book with liquidity on both sides.

    Built around a midpoint rather than drawing both sides from one range.
    Independent draws cross almost every time, so `assume(book.is_valid)`
    rejected fifty inputs out of fifty -- the analytics below would have been
    measuring nothing. Quantities start above zero so no level is dropped as a
    delete and leaves a side empty.
    """
    mid = draw(st.integers(min_value=5_000, max_value=15_000))
    offsets = st.lists(st.integers(min_value=1, max_value=400), min_size=1,
                       max_size=6, unique=True)
    qty = st.decimals(min_value=Decimal("0.01"), max_value=Decimal("50"), places=2)

    bids = tuple((Decimal(mid - o) / 100, draw(qty)) for o in draw(offsets))
    asks = tuple((Decimal(mid + o) / 100, draw(qty)) for o in draw(offsets))
    return book_from(bids, asks)


# --- the book as a state machine --------------------------------------------


class BookLifecycle(RuleBasedStateMachine):
    """Random sequences of snapshots and updates against one book.

    Hypothesis picks a rule, runs it, checks every `@invariant`, and repeats.
    Sequence numbers are drawn from a small range on purpose: it makes gaps,
    duplicates and exact contiguity all likely, where realistic numbers would
    make every update a gap and the interesting paths would never run.
    """

    def __init__(self) -> None:
        super().__init__()
        self.book = OrderBook("test", "TEST")

    @rule(bids=levels, asks=levels, seq=st.integers(min_value=1, max_value=40))
    def load_snapshot(self, bids, asks, seq) -> None:
        # No sequence expectation afterwards: a snapshot replaces the book
        # wholesale, so it may legitimately carry any sequence number.
        self.book.load_snapshot(BookSnapshot("test", "TEST", 0, 0, seq, bids, asks))

    @rule(
        bids=levels,
        asks=levels,
        first=st.integers(min_value=1, max_value=40),
        span=st.integers(min_value=0, max_value=5),
    )
    def apply_update(self, bids, asks, first, span) -> None:
        before = self.book.last_sequence
        result = self.book.apply(
            BookUpdate("test", "TEST", 0, 0, first, first + span, bids, asks)
        )
        if result is ApplyResult.APPLIED:
            # An applied update always advances. Not advancing would mean a
            # duplicate was applied rather than ignored.
            assert self.book.last_sequence > before
        elif result is ApplyResult.DUPLICATE:
            assert self.book.last_sequence == before

    @invariant()
    def a_valid_book_is_never_crossed(self) -> None:
        """The invariant the whole validity concept exists to protect."""
        if not self.book.is_valid:
            return
        bid, ask = self.book.best_bid, self.book.best_ask
        if bid is not None and ask is not None:
            assert bid < ask

    @invariant()
    def an_invalid_book_refuses_every_read(self) -> None:
        """Fail closed means all reads, not just the ones someone remembered."""
        if self.book.is_valid:
            return
        for read in (
            lambda: self.book.best_bid,
            lambda: self.book.best_ask,
            lambda: self.book.top_bids(5),
            lambda: self.book.top_asks(5),
        ):
            try:
                read()
                raise AssertionError("an invalid book served a read")
            except BookInvalidError:
                pass

    @invariant()
    def zero_quantities_are_never_stored(self) -> None:
        """A zero is a delete. One left in the book is liquidity that does not
        exist and that no later delete can clear."""
        assert all(qty != 0 for qty in self.book._bids.values())
        assert all(qty != 0 for qty in self.book._asks.values())

    @invariant()
    @precondition(lambda self: self.book.is_valid)
    def depth_is_ordered_best_first(self) -> None:
        bids = [price for price, _ in self.book.top_bids(6)]
        asks = [price for price, _ in self.book.top_asks(6)]
        assert bids == sorted(bids, reverse=True)
        assert asks == sorted(asks)

    @invariant()
    @precondition(lambda self: self.book.is_valid)
    def a_narrower_depth_is_a_prefix_of_a_wider_one(self) -> None:
        """Asking for fewer levels must return the same levels, not different
        ones. A sort that is not stable across calls would break this."""
        assert self.book.top_bids(2) == self.book.top_bids(6)[:2]
        assert self.book.top_asks(2) == self.book.top_asks(6)[:2]


TestBookLifecycle = BookLifecycle.TestCase
TestBookLifecycle.settings = settings(max_examples=200, stateful_step_count=30)


# --- analytics are bounded --------------------------------------------------


@given(book=two_sided_books(), depth=st.integers(min_value=1, max_value=10))
def test_imbalance_stays_within_its_range(book, depth) -> None:
    """A ratio that escapes [-1, 1] is meaningless, and would poison any model
    trained on it without ever raising."""
    value = imbalance(book, depth)

    assert value is not None
    assert Decimal(-1) <= value <= Decimal(1)


@given(book=two_sided_books(), depth=st.integers(min_value=1, max_value=10))
def test_depth_totals_are_never_negative(book, depth) -> None:
    bid_total, ask_total = market_depth(book, depth)

    assert bid_total > 0 and ask_total > 0


@given(book=two_sided_books())
def test_microprice_stays_inside_the_spread(book) -> None:
    """Microprice is a convex combination of the two touch prices, so it can
    never leave the spread whichever way the weights are paired.

    Which is the limit of what a property can check here: leaning the *wrong*
    way still satisfies this bound. That is why the direction is asserted
    against midprice in test_analytics, by example, with unequal sizes.
    """
    assert book.best_bid <= microprice(book) <= book.best_ask


@given(book=two_sided_books())
def test_spread_is_never_negative_on_a_valid_book(book) -> None:
    """A negative spread means a crossed book that was not detected."""
    assert spread(book) > 0


@given(book=two_sided_books())
def test_midprice_is_between_the_touch(book) -> None:
    assert book.best_bid <= midprice(book) <= book.best_ask


# --- storage round trip -----------------------------------------------------

T0 = 1_788_307_200_000_000_000

trades = st.builds(
    Trade,
    st.just("binance"),
    st.just("BTCUSDT"),
    st.integers(min_value=T0, max_value=T0 + 86_399_000_000_000),
    st.integers(min_value=T0, max_value=T0 + 86_399_000_000_000),
    prices,
    quantities,
    st.integers(min_value=0, max_value=2**31),
    st.sampled_from(Side),
)

updates = st.builds(
    BookUpdate,
    st.just("binance"),
    st.just("BTCUSDT"),
    st.integers(min_value=T0, max_value=T0 + 86_399_000_000_000),
    st.integers(min_value=T0, max_value=T0 + 86_399_000_000_000),
    st.integers(min_value=1, max_value=2**31),
    st.integers(min_value=1, max_value=2**31),
    levels,
    levels,
)

snapshots = st.builds(
    BookSnapshot,
    st.just("binance"),
    st.just("BTCUSDT"),
    st.integers(min_value=T0, max_value=T0 + 86_399_000_000_000),
    st.integers(min_value=T0, max_value=T0 + 86_399_000_000_000),
    st.integers(min_value=1, max_value=2**31),
    levels,
    levels,
)


@given(events=st.lists(st.one_of(trades, updates, snapshots), min_size=1, max_size=40))
@settings(max_examples=60, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_any_event_sequence_survives_the_round_trip(events) -> None:
    """Written to Parquet and read back, every event equals what went in.

    Dataclass equality, so this compares every field including Decimals.
    Replay's whole value rests on this holding for sequences nobody wrote by
    hand, not just the ones in test_replay.
    """
    with tempfile.TemporaryDirectory() as directory:
        with EventStore(directory, "binance", "BTCUSDT", session=1) as store:
            for event in events:
                store.write(event)

        recovered: list = []
        for date in sorted(p.name for p in Path(directory, "binance/BTCUSDT").iterdir()):
            recovered.extend(read_partition(directory, "binance", "BTCUSDT", date))

    assert recovered == events


@given(events=st.lists(st.one_of(trades, updates), min_size=1, max_size=20))
@settings(max_examples=40, deadline=None, suppress_health_check=[HealthCheck.too_slow])
def test_capture_seq_is_dense_and_ordered(events) -> None:
    """Replay's ordering key must have no holes and no repeats, whatever mix
    of streams it was spread across."""
    with tempfile.TemporaryDirectory() as directory:
        with EventStore(directory, "binance", "BTCUSDT", session=1) as store:
            for event in events:
                store.write(event)

        seen = []
        for path in Path(directory).rglob("*.parquet"):
            if path.stem.startswith("features"):
                continue
            seen.extend(pq.read_table(path).column("capture_seq").to_pylist())

    assert sorted(seen) == list(range(len(events)))


# --- the book accepts what it should ----------------------------------------


@given(
    bids=levels,
    asks=levels,
    seq=st.integers(min_value=1, max_value=1000),
    span=st.integers(min_value=0, max_value=5),
)
def test_a_contiguous_update_is_always_applied(bids, asks, seq, span) -> None:
    """The complement of the fail-closed tests: correct input must not be
    rejected. A book that refuses valid updates resynchronises forever and
    looks, from the outside, exactly like a quiet market.
    """
    book = book_from(bids, asks)
    assume(book.state is BookState.SEEDED)

    result = book.apply(
        BookUpdate("test", "TEST", 0, 0, seq + 1, seq + 1 + span, (), ())
    )
    assume(result is not ApplyResult.DUPLICATE)

    # The book was seeded at sequence 1, so anything starting above 2 is a
    # genuine gap; only the spanning case should apply.
    assert result is (ApplyResult.APPLIED if seq + 1 <= 2 else ApplyResult.GAP)
