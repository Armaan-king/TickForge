"""Normalized market events.

Adapters translate exchange-specific formats into these types. Nothing
downstream of an adapter should know which venue produced an event — see
docs/knowledge/architecture.md.
"""

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

PriceLevel = tuple[Decimal, Decimal]
"""(price, quantity). A quantity of 0 means the level is removed."""


class Side(StrEnum):
    """The side that crossed the spread to make a trade happen.

    A `StrEnum` rather than the `Enum`/`auto()` pattern in `book.py`: those are
    internal states that never leave the process, while this one gets written
    to Parquet in Phase 5. A `StrEnum` member *is* its string, so it lands in a
    string column with no conversion and survives the round trip unchanged;
    `auto()` would store an integer whose meaning lives only in this file.
    """

    BUY = "buy"
    SELL = "sell"


@dataclass(frozen=True, slots=True)
class BookUpdate:
    """An incremental change to the L2 book.

    Covers a contiguous range of sequence numbers, `first_seq`..`last_seq`
    inclusive. Exchanges batch many level changes into one update — Binance
    sends 57-782 per message on BTCUSDT.

    Attributes:
        exchange: Origin venue. Downstream may record it but must not branch
            on it.
        symbol: Venue-neutral instrument identifier.
        timestamp_ns: Exchange event time, nanoseconds since epoch.
        received_ns: Local receive time, nanoseconds. Compared against
            timestamp_ns to detect a feed that is open but stale.
        first_seq: First sequence number covered by this update.
        last_seq: Last sequence number covered, inclusive.
        bids: Bid-side level changes.
        asks: Ask-side level changes.
    """

    exchange: str
    symbol: str
    timestamp_ns: int
    received_ns: int
    first_seq: int
    last_seq: int
    bids: tuple[PriceLevel, ...]
    asks: tuple[PriceLevel, ...]


@dataclass(frozen=True, slots=True)
class BookSnapshot:
    """Complete book state at a single sequence number.

    Unlike `BookUpdate` this carries no range: a snapshot is the whole book as
    of `last_seq`, not a change covering a span. It is modelled as an event so
    that replay can replay a resynchronisation, which a direct method call on
    the book could not.

    Attributes:
        exchange: Origin venue.
        symbol: Venue-neutral instrument identifier.
        timestamp_ns: Exchange time if the venue supplies one. REST snapshot
            endpoints typically do not, in which case this mirrors
            `received_ns` -- see the adapter that produced it.
        received_ns: Local receive time, nanoseconds.
        last_seq: The sequence number this state is current as of.
        bids: Complete bid side, to whatever depth was requested.
        asks: Complete ask side.
    """

    exchange: str
    symbol: str
    timestamp_ns: int
    received_ns: int
    last_seq: int
    bids: tuple[PriceLevel, ...]
    asks: tuple[PriceLevel, ...]

@dataclass(frozen=True, slots=True)
class Trade:
    """A single execution.

    Not book state. A trade carries no sequence range and no contiguity
    requirement, because a missed trade cannot corrupt anything the way a
    missed book update can -- it is simply lost. Nothing downstream needs to
    halt over one.

    Attributes:
        exchange: Origin venue. Downstream may record it but must not branch
            on it.
        symbol: Venue-neutral instrument identifier.
        timestamp_ns: When the match happened, nanoseconds since epoch -- not
            when the venue emitted the frame. Binance sends both (`T` and `E`)
            and the gap between them is server-side queuing, which carries no
            market information. Analytics line trades up against book state by
            this value, so it has to be the match time. `BookUpdate` uses the
            emission time only because a depth frame has no separate moment of
            occurrence; a trade does.
        received_ns: Local receive time, nanoseconds.
        price: Execution price.
        quantity: Base-asset amount filled.
        trade_id: Venue trade identifier. Present for deduplication after a
            reconnect, not for ordering -- nothing requires it to be gap-free,
            and no component may halt on a jump in it.
        aggressor: The side that crossed the spread. Derived, never copied:
            Binance reports `m`, "was the buyer the market maker?", and that
            field must not survive the adapter -- see architecture.md. `m`
            true means the buyer's order was already resting on the book, so
            the *seller* crossed, and the trade is `Side.SELL`. Getting this
            backwards is invisible -- both directions produce plausible
            order-flow imbalance in Phase 4, one of them sign-flipped.
    """

    exchange: str
    symbol: str
    timestamp_ns: int
    received_ns: int
    price: Decimal
    quantity: Decimal
    trade_id: int
    aggressor: Side


MarketEvent = BookUpdate | BookSnapshot | Trade
"""Anything the pipeline carries. Widen as event types are added.

Widening this is an API change, not an addition: every `isinstance` dispatch
downstream gains a case it does not handle, and the ones written as
if-snapshot-else-update silently start treating the new type as an update.
"""
