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
