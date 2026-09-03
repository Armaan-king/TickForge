"""Normalized market events.

Adapters translate exchange-specific formats into these types. Nothing
downstream of an adapter should know which venue produced an event — see
docs/knowledge/architecture.md.
"""

from dataclasses import dataclass
from decimal import Decimal

PriceLevel = tuple[Decimal, Decimal]
"""(price, quantity). A quantity of 0 means the level is removed."""


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


MarketEvent = BookUpdate | BookSnapshot
"""Anything the pipeline carries. Widen as event types are added."""
