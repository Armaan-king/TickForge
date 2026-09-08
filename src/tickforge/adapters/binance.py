"""Binance adapter.

The only module permitted to know Binance's wire format. Its field names --
``U``, ``u``, ``b``, ``a`` -- must not appear anywhere else in the codebase.

Wire format of a @depth frame::

    {"e": "depthUpdate",        event type
     "E": 1788376136014,        event time, MILLISECONDS
     "s": "BTCUSDT",            symbol
     "U": 99588317538,          first update id in this frame
     "u": 99588317663,          final update id in this frame
     "b": [["77381.36", "1.29"], ...],   bid levels, [price, qty] as strings
     "a": [["77381.37", "3.16"], ...]}   ask levels

Wire format of a @trade frame::

    {"e": "trade",              event type
     "E": 1788376136014,        event time -- when the SERVER sent this
     "s": "BTCUSDT",            symbol
     "t": 4183726,              trade id
     "p": "77381.36",           price
     "q": "0.00214",            quantity
     "T": 1788376136011,        trade time -- when the MATCH happened
     "m": false}                was the buyer the market maker?

A combined subscription wraps either of those in an envelope::

    {"stream": "btcusdt@depth", "data": { ... }}

A quantity of ``"0"`` means the level is removed. Prices and quantities arrive
as decimal strings so the exact value survives; they are converted straight to
Decimal, never via float.
"""

import json
import time
from decimal import Decimal

from tickforge.events import BookSnapshot, BookUpdate, PriceLevel, Side, Trade

EXCHANGE = "binance"
SNAPSHOT_URL = "https://api.binance.com/api/v3/depth"


def _levels(raw_levels: list[list[str]]) -> tuple[PriceLevel, ...]:
    """Convert wire levels to Decimal pairs, preserving zero quantities.

    Zero-quantity levels are deletions and must reach the book -- dropping
    them here would leave phantom liquidity that never clears.
    """
    return tuple((Decimal(price), Decimal(quantity)) for price, quantity in raw_levels)


def _depth_update(message: dict, received_ns: int) -> BookUpdate:
    """Build a `BookUpdate` from an already-decoded ``depthUpdate`` body."""
    return BookUpdate(
        exchange=EXCHANGE,
        symbol=message["s"],
        timestamp_ns=message["E"] * 1_000_000,
        received_ns=received_ns,
        first_seq=message["U"],
        last_seq=message["u"],
        bids=_levels(message["b"]),
        asks=_levels(message["a"]),
    )


def _trade(message: dict, received_ns: int) -> Trade:
    """Build a `Trade` from an already-decoded ``trade`` body."""
    return Trade(
        exchange=EXCHANGE,
        symbol=message["s"],
        # "T", the match time, not "E", when the server sent the frame. The
        # gap between them is queuing and carries no market information.
        timestamp_ns=message["T"] * 1_000_000,
        received_ns=received_ns,
        price=Decimal(message["p"]),
        quantity=Decimal(message["q"]),
        trade_id=message["t"],
        # "m" is "was the buyer the maker?", so true means the buyer was
        # already resting and the SELLER crossed. The inversion stops here.
        aggressor=Side.SELL if message["m"] else Side.BUY,
    )


def parse_depth_update(raw: str | bytes, received_ns: int | None = None) -> BookUpdate:
    """Translate one Binance ``depthUpdate`` frame into a `BookUpdate`.

    Args:
        raw: A single WebSocket text frame from the ``@depth`` stream.
        received_ns: Local receive time in nanoseconds. Defaults to now, but
            pass the value stamped when the frame actually arrived -- if
            parsing is deferred or batched, the delay would otherwise be
            misread as feed latency.

    Returns:
        The normalized update.

    Raises:
        ValueError: The frame is not a ``depthUpdate``.
        KeyError: A required field is absent.
    """
    if received_ns is None:
        received_ns = time.time_ns()

    message = json.loads(raw)

    event_type = message.get("e")
    if event_type != "depthUpdate":
        raise ValueError(f"expected a depthUpdate frame, got {event_type!r}")

    return _depth_update(message, received_ns)


def parse_trade(raw: str | bytes, received_ns: int | None = None) -> Trade:
    """Translate one Binance ``trade`` frame into a `Trade`.

    Args:
        raw: A single WebSocket text frame from the ``@trade`` stream.
        received_ns: Local receive time in nanoseconds. Defaults to now.

    Returns:
        The normalized trade, with Binance's maker flag already resolved into
        an aggressor `Side`.

    Raises:
        ValueError: The frame is not a ``trade``.
        KeyError: A required field is absent.
    """
    if received_ns is None:
        received_ns = time.time_ns()

    message = json.loads(raw)

    event_type = message.get("e")
    if event_type != "trade":
        raise ValueError(f"expected a trade frame, got {event_type!r}")

    return _trade(message, received_ns)


def parse_stream_frame(
    raw: str | bytes, received_ns: int | None = None
) -> BookUpdate | Trade:
    """Translate one frame from a *combined* subscription.

    A combined stream multiplexes several subscriptions onto one socket, so
    the venue decides the interleaving of trades and depth updates rather than
    the client inventing an order by merging two connections.

    Args:
        raw: A frame from ``/stream?streams=...``, wrapped in the envelope.
        received_ns: Local receive time in nanoseconds. Defaults to now.

    Returns:
        Whichever event the frame carried.

    Raises:
        ValueError: The envelope is malformed, or carries an event type this
            adapter does not handle.
        KeyError: A required field is absent.
    """
    if received_ns is None:
        received_ns = time.time_ns()

    envelope = json.loads(raw)
    if "data" not in envelope:
        raise ValueError(f"not a combined-stream frame: {envelope}")

    message = envelope["data"]
    event_type = message.get("e")
    if event_type == "depthUpdate":
        return _depth_update(message, received_ns)
    if event_type == "trade":
        return _trade(message, received_ns)
    raise ValueError(f"unsupported event type {event_type!r}")


def parse_snapshot(
    raw: str | bytes, symbol: str, received_ns: int | None = None
) -> BookSnapshot:
    """Translate a REST ``/api/v3/depth`` response into a `BookSnapshot`.

    The REST shape differs from the stream in three ways worth knowing:
    it does not echo the symbol, it carries no event type, and it has no
    exchange timestamp. So the symbol must be supplied by the caller, and
    ``timestamp_ns`` mirrors ``received_ns`` because no exchange time exists::

        {"lastUpdateId": 99588317538,
         "bids": [["77381.36000000", "1.29993000"], ...],
         "asks": [["77381.37000000", "3.16444000"], ...]}

    Args:
        raw: The response body.
        symbol: Instrument the snapshot was requested for. The response does
            not contain it.
        received_ns: Local receive time in nanoseconds. Defaults to now.

    Returns:
        The normalized snapshot.

    Raises:
        ValueError: The body is not a depth snapshot -- typically a Binance
            error object such as ``{"code": -1121, "msg": "Invalid symbol."}``.
    """
    if received_ns is None:
        received_ns = time.time_ns()

    message = json.loads(raw)

    if "lastUpdateId" not in message:
        detail = message.get("msg", message)
        raise ValueError(f"not a depth snapshot: {detail}")

    return BookSnapshot(
        exchange=EXCHANGE,
        symbol=symbol,
        timestamp_ns=received_ns,
        received_ns=received_ns,
        last_seq=message["lastUpdateId"],
        bids=_levels(message["bids"]),
        asks=_levels(message["asks"]),
    )
