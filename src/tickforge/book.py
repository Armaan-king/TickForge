"""In-memory Level-2 order book for one instrument on one venue.

A pure state machine over market events: no parsing, no network, no knowledge
of any exchange. That is what makes it testable without a connection and
identical under live and replay -- see docs/knowledge/architecture.md.
"""

import heapq
from decimal import Decimal
from enum import Enum, auto

from tickforge.events import BookSnapshot, BookUpdate, PriceLevel


class BookState(Enum):
    """Where the book is in its synchronisation lifecycle."""

    EMPTY = auto()
    """No snapshot yet. Knows nothing; cannot be read."""
    SEEDED = auto()
    """Snapshot loaded. Complete and readable, awaiting the update whose range
    spans the snapshot boundary."""
    SYNCED = auto()
    """Steady state. Each update must be exactly contiguous with the last."""
    INVALID = auto()
    """A gap or a crossed book was seen. Must be resynchronised."""


class ApplyResult(Enum):
    """Outcome of applying an update.

    Returned rather than raised: sequence gaps are a normal operating
    condition, not an exceptional one. Reads fail closed independently, so an
    ignored result still cannot produce plausible-looking wrong numbers.
    """

    APPLIED = auto()
    DUPLICATE = auto()
    """Entirely before the current position. Ignored; the book is untouched."""
    GAP = auto()
    """Updates were missed. The book is now invalid and must resynchronise."""
    CROSSED = auto()
    """Applied, but produced bid >= ask. The book is invalid."""
    NOT_SYNCED = auto()
    """No snapshot loaded, or the book is invalid. Nothing was applied."""


class BookInvalidError(RuntimeError):
    """Raised on reading a book that is not in sync."""


class OrderBook:
    """Aggregated liquidity per price level, maintained from market events."""

    def __init__(self, exchange: str, symbol: str) -> None:
        self.exchange = exchange
        self.symbol = symbol
        self._bids: dict[Decimal, Decimal] = {}
        self._asks: dict[Decimal, Decimal] = {}
        self._last_seq: int | None = None
        self._state = BookState.EMPTY

    def load_snapshot(self, snapshot: BookSnapshot) -> BookState:
        """Replace all state with a complete snapshot.

        Wholesale replacement, not a merge: the snapshot *is* the book as of
        its sequence number, and merging would preserve stale levels that the
        exchange no longer reports.
        """
        if (snapshot.exchange, snapshot.symbol) != (self.exchange, self.symbol):
            raise ValueError(
                f"snapshot is for {snapshot.exchange}:{snapshot.symbol}, "
                f"book is {self.exchange}:{self.symbol}"
            )

        # Zero quantities should not appear in a snapshot; if the venue sends
        # one it would become a phantom level that no delete ever clears.
        self._bids = {price: qty for price, qty in snapshot.bids if qty != 0}
        self._asks = {price: qty for price, qty in snapshot.asks if qty != 0}
        self._last_seq = snapshot.last_seq
        self._state = BookState.INVALID if self._is_crossed() else BookState.SEEDED
        return self._state

    def apply(self, update: BookUpdate) -> ApplyResult:
        """Apply one incremental update.

        Sequencing is checked against the lifecycle state. Immediately after a
        snapshot the first update must *span* the boundary rather than start
        exactly at it, because the snapshot lands partway through an update's
        range. In steady state exact contiguity is required.
        """
        if self._state in (BookState.EMPTY, BookState.INVALID):
            return ApplyResult.NOT_SYNCED

        assert self._last_seq is not None  # guaranteed by EMPTY check above

        if update.last_seq <= self._last_seq:
            return ApplyResult.DUPLICATE

        expected = self._last_seq + 1
        contiguous = (
            update.first_seq <= expected
            if self._state is BookState.SEEDED
            else update.first_seq == expected
        )
        if not contiguous:
            self._state = BookState.INVALID
            return ApplyResult.GAP

        self._apply_levels(self._bids, update.bids)
        self._apply_levels(self._asks, update.asks)
        self._last_seq = update.last_seq
        self._state = BookState.SYNCED

        if self._is_crossed():
            self._state = BookState.INVALID
            return ApplyResult.CROSSED

        return ApplyResult.APPLIED

    @staticmethod
    def _apply_levels(side: dict[Decimal, Decimal], levels: tuple[PriceLevel, ...]) -> None:
        """Set or remove levels. Quantity 0 is the delete signal.

        A delete for an unknown price is a no-op, not an error: exchanges
        routinely report removals for levels outside the depth being tracked.
        """
        for price, quantity in levels:
            if quantity == 0:
                side.pop(price, None)
            else:
                side[price] = quantity

    def _is_crossed(self) -> bool:
        """True if the best bid is at or above the best ask, which cannot happen
        in a real market and therefore means the local state is wrong."""
        if not self._bids or not self._asks:
            return False
        # ponytail: O(n) scan per update. Fine at current volumes; if it shows
        # up in a Phase 9 profile, track best bid/ask incrementally instead.
        return max(self._bids) >= min(self._asks)

    @property
    def state(self) -> BookState:
        return self._state

    @property
    def is_valid(self) -> bool:
        """True while the book may be read. A seeded book is complete and
        correct even before its first update."""
        return self._state in (BookState.SEEDED, BookState.SYNCED)

    @property
    def last_sequence(self) -> int | None:
        return self._last_seq

    @property
    def best_bid(self) -> Decimal | None:
        self._require_valid()
        return max(self._bids) if self._bids else None

    @property
    def best_ask(self) -> Decimal | None:
        self._require_valid()
        return min(self._asks) if self._asks else None

    def top_bids(self, n: int) -> tuple[PriceLevel, ...]:
        """The `n` bid levels nearest the spread, highest price first.

        Returns fewer than `n` entries when the book holds fewer. A thin book
        is not an error, but a feature averaged over it is measuring something
        narrower than it claims, so a caller that cares must check the length.

        Raises:
            BookInvalidError: The book is not in sync.
        """
        self._require_valid()
        # ponytail: O(n log k) per call. Cheaper than sorting all levels, and
        # if Phase 9 shows it dominating, keep the two sides sorted instead.
        prices = heapq.nlargest(n, self._bids)
        return tuple((price, self._bids[price]) for price in prices)

    def top_asks(self, n: int) -> tuple[PriceLevel, ...]:
        """The `n` ask levels nearest the spread, lowest price first.

        Mirror of `top_bids`. Both are best-first so that a feature computed
        over one side is symmetric with the other -- imbalance never has to
        know which side it is looking at.

        Raises:
            BookInvalidError: The book is not in sync.
        """
        self._require_valid()
        prices = heapq.nsmallest(n, self._asks)
        return tuple((price, self._asks[price]) for price in prices)

    def _require_valid(self) -> None:
        if not self.is_valid:
            raise BookInvalidError(
                f"{self.exchange}:{self.symbol} book is {self._state.name} "
                f"(last sequence {self._last_seq}); resynchronise before reading"
            )
