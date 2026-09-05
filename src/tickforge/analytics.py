"""Microstructure features derived from book state.

Pure functions, deliberately: a feature is a function of the book at one
instant, nothing more. Nothing here writes to the book -- see
docs/knowledge/architecture.md. Every function returns None rather than
raising when the book is one-sided, because a market with no offers is a real
state, not an error. An *invalid* book is a different matter: the reads these
call fail closed and raise BookInvalidError, which is intended.
"""

from decimal import Decimal

from tickforge.book import OrderBook


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