"""Microstructure features.

Two kinds live here, and the split is the design.

The **functions** are pure: a feature of the book at one instant, nothing more.
Nothing here writes to the book -- see docs/knowledge/architecture.md. They
return None rather than raising when the book is one-sided, because a market
with no offers is a real state, not an error. An *invalid* book is different:
the reads they call fail closed and raise BookInvalidError, which is intended.

`FlowFeatures` is the other kind: features that cannot be computed from one
instant because they measure change or accumulation. It holds a rolling
window, which makes it the first thing in this project with a clock -- and
that clock is event time, never wall time. See its docstring.
"""

from collections import deque
from decimal import Decimal

from tickforge.book import OrderBook
from tickforge.events import BookSnapshot, MarketEvent, Side, Trade


def spread(book: OrderBook) -> Decimal | None:
    """Distance between the best bid and the best ask.

    Returns:
        A positive Decimal, or None if either side is empty.
    """
    best_ask=book.best_ask
    best_bid=book.best_bid
    if best_ask is None or best_bid is None:
        return None
    spread=best_ask-best_bid
    return spread


def midprice(book: OrderBook) -> Decimal | None:
    """The naive fair value: halfway between the best bid and ask.

    Ignores how much size rests on each side, so it sits exactly in the middle
    of a book with one lot bid and a thousand offered. `microprice` is the
    size-aware version.

    Returns:
        The midpoint, or None if either side is empty.
    """
    best_bid=book.best_bid
    best_ask=book.best_ask
    if best_bid is None or best_ask is None:
        return None
    return (best_bid + best_ask) / 2


def microprice(book: OrderBook) -> Decimal | None:
    """Size-weighted fair value, leaning away from the heavier side.

    Returns:
        A price between the best bid and the best ask, or None if either side
        is empty. Equal size on both sides gives exactly the midprice.
    """
    bids=book.top_bids(1)
    asks=book.top_asks(1)
    if not bids or not asks:
        return None
    bid_px, bid_qty=bids[0]
    ask_px, ask_qty=asks[0]
    # CROSSED on purpose: each price is weighted by the OTHER side's size.
    # A big wall of offers is hard to buy through and easy to sell into, so
    # price runs away from the heavy side -- and the heavy side's own quantity
    # is what has to be the weight on the opposite price.
    # Pairing each price with its own quantity looks like the fix, and is the
    # bug: still lands between bid and ask, just leaning the wrong way.
    mcp=(bid_px*ask_qty+ask_px*bid_qty)/(bid_qty+ask_qty)
    return mcp

def market_depth(book: OrderBook, depth: int) -> tuple[Decimal,Decimal]:
    bids=book.top_bids(depth)
    asks=book.top_asks(depth)
    bid_total=sum((qty for _,qty in bids),Decimal(0))
    ask_total=sum((qty for _,qty in asks),Decimal(0))
    return (bid_total,ask_total)

def imbalance(book: OrderBook, depth: int) -> Decimal | None:
    bids=book.top_bids(depth)
    asks=book.top_asks(depth)
    bid_total, ask_total = market_depth(book, depth)
    if bid_total+ask_total==0:
        return None
    imb=(bid_total-ask_total)/(bid_total+ask_total)
    return imb


# --- features that need history ---------------------------------------------

L1 = tuple[Decimal, Decimal, Decimal, Decimal]
"""(bid_price, bid_quantity, ask_price, ask_quantity) -- the touch."""


def _touch(book: OrderBook) -> L1 | None:
    """Both sides of the touch, or None if either side is empty."""
    bids = book.top_bids(1)
    asks = book.top_asks(1)
    if not bids or not asks:
        return None
    return (*bids[0], *asks[0])


class FlowFeatures:
    """Rolling-window features that measure change or accumulation.

    Unlike the functions above, these cannot be read off a single book state:
    order-flow imbalance compares consecutive states, and the rest aggregate
    over a period. So this holds a window -- and therefore a clock.

    **That clock is event time.** The window is measured against
    `event.timestamp_ns`, never `time.time()`. This is not a style preference:
    replay pushes hours of recorded events through in seconds, so a wall clock
    would place every recorded event outside the window and quietly return
    nothing for the entire replay, having worked perfectly live. Deterministic
    replay is the Phase 6 guarantee and this class is the first thing in the
    project capable of breaking it. See docs/knowledge/pitfalls.md.

    The consequence is deliberate: if the feed stalls, event time stops and
    the window stops expiring. That is honest -- no events means no
    information about that period, and ageing data out on a clock nothing
    observed would be inventing knowledge. Noticing the stall is a separate
    job, and what `received_ns` exists for.

    Call `observe` once per event, *after* applying it to the book.
    """

    def __init__(self, window_ns: int) -> None:
        self._window_ns = window_ns
        self._now = 0
        # deque, not list. The window is strictly FIFO -- newest in at the
        # right, oldest out at the left -- and `list.pop(0)` is O(n) because a
        # list is one contiguous array, so removing the head relocates
        # everything behind it. `deque.popleft()` is O(1). BTCUSDT runs ~33
        # trades a second, so a 60s window holds ~2000 entries and evicts
        # roughly once per arrival: 2000 element shifts per event with a list,
        # one pointer move with a deque.
        #
        # The items are stored rather than folded into a running sum because
        # subtracting an expired entry means knowing what it was. The deque is
        # that record.
        self._trades: deque[Trade] = deque()
        # (timestamp, contribution) rather than raw state: both of these are
        # per-update quantities, so evicting the oldest entry is the whole of
        # rolling the window forward.
        #
        # ponytail: reads are O(n) over the window. If a Phase 9 profile shows
        # that dominating, keep running totals and subtract on eviction --
        # O(1) reads, at the cost of the totals and the deque being able to
        # disagree.
        self._ofi: deque[tuple[int, Decimal]] = deque()
        self._returns: deque[tuple[int, Decimal]] = deque()
        self._prev_touch: L1 | None = None
        self._prev_mid: Decimal | None = None
        self._tracking = False
        self._holed_until: int | None = None

    def observe(self, event: MarketEvent, book: OrderBook) -> None:
        """Record one event. Call after the book has been updated with it.

        Args:
            event: Any market event. Trades accumulate; book updates produce
                an order-flow and a return contribution; a snapshot resets the
                continuity-dependent state.
            book: The book *after* this event was applied, read for its touch.
                Reading it here rather than tracking L1 independently avoids a
                second implementation of something `book.py` already gets right.
        """
        # Never let the clock run backwards. Trades are stamped at match time
        # and depth frames at emission time, so the two interleave slightly
        # out of order; a receding cutoff would widen the window at random.
        self._now = max(self._now, event.timestamp_ns)

        if isinstance(event, Trade):
            self._trades.append(event)
        elif isinstance(event, BookSnapshot):
            self._resynced(book)
        else:
            self._observe_book(book)

        self._evict()

    def _resynced(self, book: OrderBook) -> None:
        """Drop everything that assumed an unbroken sequence of book states.

        A snapshot means an unknown number of updates went missing. Order flow
        and returns summed across that hole would understate the period while
        looking entirely normal -- the silent-corruption failure this project
        exists to avoid. Trades survive: each one is self-contained, and the
        ones already recorded really did happen.

        Clearing is not enough on its own. An emptied window reads as zero,
        and zero means "no net flow" -- a calm, confident number, produced for
        a full window immediately after the one event that says the market was
        probably doing something. So the window is also marked incomplete
        until it has refilled entirely from after the gap, and the two
        continuity-dependent features report None until then.

        A *first* snapshot is not a resync: nothing was missed, observation
        simply starts here. `_tracking` tells the two apart, because the feed
        deliberately does not signal a resync -- applying the new snapshot is
        the whole of the recovery.
        """
        if self._tracking:
            self._holed_until = self._now + self._window_ns
        self._ofi.clear()
        self._returns.clear()
        self._prev_touch = _touch(book)
        self._prev_mid = midprice(book)

    @property
    def _holed(self) -> bool:
        """True while the window still spans a resynchronisation."""
        return self._holed_until is not None and self._now < self._holed_until

    def _observe_book(self, book: OrderBook) -> None:
        """Turn a book update into an order-flow and a return contribution."""
        self._tracking = True
        touch = _touch(book)
        mid = midprice(book)

        if touch is not None and self._prev_touch is not None:
            self._ofi.append((self._now, _order_flow(self._prev_touch, touch)))
        if mid is not None and self._prev_mid is not None and mid != self._prev_mid:
            log_return = (mid / self._prev_mid).ln()
            self._returns.append((self._now, log_return * log_return))

        self._prev_touch = touch
        self._prev_mid = mid

    def _evict(self) -> None:
        """Drop everything older than the window, measured from `_now`."""
        cutoff = self._now - self._window_ns
        while self._trades and self._trades[0].timestamp_ns < cutoff:
            self._trades.popleft()
        while self._ofi and self._ofi[0][0] < cutoff:
            self._ofi.popleft()
        while self._returns and self._returns[0][0] < cutoff:
            self._returns.popleft()

    @property
    def vwap(self) -> Decimal | None:
        """Volume-weighted average traded price over the window.

        Returns:
            None if nothing traded. Zero trades is not a price of zero.
        """
        volume = sum((t.quantity for t in self._trades), Decimal(0))
        if volume == 0:
            return None
        notional = sum((t.price * t.quantity for t in self._trades), Decimal(0))
        return notional / volume

    @property
    def trade_imbalance(self) -> Decimal | None:
        """Signed share of traded volume that lifted offers rather than hit bids.

        The trade-flow counterpart to `imbalance`, which measures resting size.
        One says what people did, the other what they are offering to do.

        Returns:
            [-1, 1]; +1 is all buying. None if nothing traded.
        """
        bought = sum(
            (t.quantity for t in self._trades if t.aggressor is Side.BUY), Decimal(0)
        )
        sold = sum(
            (t.quantity for t in self._trades if t.aggressor is Side.SELL), Decimal(0)
        )
        if bought + sold == 0:
            return None
        return (bought - sold) / (bought + sold)

    @property
    def order_flow_imbalance(self) -> Decimal | None:
        """Net size added at the bid minus net size added at the ask.

        Returns:
            Signed size, not a ratio -- positive is net buying pressure. Zero
            on an empty window means "no net flow", not "unknown": no observed
            updates is genuinely no observed flow. None is the unknown case,
            and means the window still spans a resynchronisation.
        """
        if self._holed:
            return None
        return sum((e for _, e in self._ofi), Decimal(0))

    @property
    def realised_volatility(self) -> Decimal | None:
        """Root of the summed squared log mid-returns over the window.

        Not annualised: scaling depends on a trading-calendar convention, and
        crypto's 24/7 clock makes the equity convention wrong. Callers that
        want an annual figure supply their own factor.

        Returns:
            None until at least one price change has been seen, and again
            while the window spans a resynchronisation.
        """
        if self._holed or not self._returns:
            return None
        return sum((r for _, r in self._returns), Decimal(0)).sqrt()


def _order_flow(prev: L1, current: L1) -> Decimal:
    """One update's contribution to order-flow imbalance (Cont et al.).

    Reads as four cases per side. On the bid: a higher price is new buying
    interest, so the whole new size counts; a lower price means the old bid was
    consumed or pulled, so the old size counts against; an unchanged price
    contributes only the change in size. The ask mirrors it with the sign
    flipped, because size arriving at the ask is selling pressure.
    """
    bid_px, bid_qty, ask_px, ask_qty = current
    prev_bid_px, prev_bid_qty, prev_ask_px, prev_ask_qty = prev

    flow = Decimal(0)
    if bid_px >= prev_bid_px:
        flow += bid_qty
    if bid_px <= prev_bid_px:
        flow -= prev_bid_qty
    if ask_px <= prev_ask_px:
        flow -= ask_qty
    if ask_px >= prev_ask_px:
        flow += prev_ask_qty
    return flow