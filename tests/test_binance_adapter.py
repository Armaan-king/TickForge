"""Binance adapter translates the wire format without losing information.

The sample below is a real captured frame, trimmed to a few levels. It is
embedded rather than read from data/ because captures are gitignored.
"""

from decimal import Decimal

import pytest

from tickforge.adapters.binance import (
    parse_depth_update,
    parse_snapshot,
    parse_stream_frame,
    parse_trade,
)
from tickforge.events import BookUpdate, Side, Trade

FRAME = (
    '{"e":"depthUpdate","E":1788376136014,"s":"BTCUSDT",'
    '"U":99588317538,"u":99588317663,'
    '"b":[["77381.36000000","1.29993000"],["77381.35000000","0.00084000"]],'
    '"a":[["77381.37000000","3.16444000"],["77381.83000000","0.00000000"]]}'
)

RECEIVED_NS = 1788376136020_000_000


def parsed():
    return parse_depth_update(FRAME, received_ns=RECEIVED_NS)


def test_identity_and_sequence_range() -> None:
    update = parsed()
    assert update.exchange == "binance"
    assert update.symbol == "BTCUSDT"
    assert update.first_seq == 99588317538
    assert update.last_seq == 99588317663


def test_event_time_converted_milliseconds_to_nanoseconds() -> None:
    assert parsed().timestamp_ns == 1788376136014_000_000


def test_received_time_is_preserved_not_regenerated() -> None:
    assert parsed().received_ns == RECEIVED_NS


def test_decimal_precision_survives_exactly() -> None:
    """The reason Decimal was chosen -- float would fail this."""
    price, quantity = parsed().bids[0]
    assert price == Decimal("77381.36000000")
    assert quantity == Decimal("1.29993000")
    assert price != float("77381.36")


def test_zero_quantity_level_is_kept() -> None:
    """Zero quantity is the delete signal; dropping it leaves phantom levels."""
    _, quantity = parsed().asks[-1]
    assert quantity == Decimal("0")


def test_levels_are_immutable() -> None:
    update = parsed()
    assert isinstance(update.bids, tuple)
    with pytest.raises(AttributeError):
        update.bids.append((Decimal("1"), Decimal("1")))


def test_rejects_non_depth_frame() -> None:
    with pytest.raises(ValueError, match="depthUpdate"):
        parse_depth_update('{"e":"trade","E":1,"s":"BTCUSDT"}')


# --- REST snapshots ---------------------------------------------------------

SNAPSHOT = (
    '{"lastUpdateId":99588317538,'
    '"bids":[["77381.36000000","1.29993000"],["77381.35000000","0.00084000"]],'
    '"asks":[["77381.37000000","3.16444000"]]}'
)


def test_snapshot_takes_symbol_from_caller() -> None:
    """The REST response does not echo the symbol back."""
    snap = parse_snapshot(SNAPSHOT, symbol="BTCUSDT", received_ns=RECEIVED_NS)
    assert snap.symbol == "BTCUSDT"
    assert snap.exchange == "binance"
    assert snap.last_seq == 99588317538


def test_snapshot_timestamp_mirrors_receive_time() -> None:
    """REST carries no exchange event time, so there is nothing else to use."""
    snap = parse_snapshot(SNAPSHOT, symbol="BTCUSDT", received_ns=RECEIVED_NS)
    assert snap.timestamp_ns == RECEIVED_NS == snap.received_ns


def test_snapshot_preserves_decimal_precision() -> None:
    snap = parse_snapshot(SNAPSHOT, symbol="BTCUSDT", received_ns=RECEIVED_NS)
    assert snap.bids[0] == (Decimal("77381.36000000"), Decimal("1.29993000"))


def test_binance_error_body_is_rejected() -> None:
    """A bad symbol returns 200 with an error object, not an HTTP error."""
    with pytest.raises(ValueError, match="Invalid symbol"):
        parse_snapshot('{"code":-1121,"msg":"Invalid symbol."}', symbol="NOPE")


# --- trades -----------------------------------------------------------------
#
# "E" and "T" differ by 3ms in this sample on purpose: a parser reading the
# wrong one still produces a plausible timestamp, so only a fixture where they
# disagree can catch it.

TRADE = (
    '{"e":"trade","E":1788376136014,"s":"BTCUSDT","t":4183726,'
    '"p":"77381.36000000","q":"0.00214000","T":1788376136011,"m":false}'
)


def test_trade_identity_and_id() -> None:
    executed = parse_trade(TRADE, received_ns=RECEIVED_NS)
    assert executed.exchange == "binance"
    assert executed.symbol == "BTCUSDT"
    assert executed.trade_id == 4183726
    assert executed.received_ns == RECEIVED_NS


def test_trade_uses_match_time_not_emission_time() -> None:
    """`T` is when the match happened, `E` when the server sent the frame.

    Analytics line trades up against book state by this value, so the 3ms of
    server-side queuing in between must not be baked into it.
    """
    assert parse_trade(TRADE).timestamp_ns == 1788376136011_000_000


def test_trade_decimal_precision_survives_exactly() -> None:
    executed = parse_trade(TRADE)
    assert executed.price == Decimal("77381.36000000")
    assert executed.quantity == Decimal("0.00214000")


def test_resting_buyer_means_the_seller_was_the_aggressor() -> None:
    """`m` true: the buyer was the market maker, already on the book. So the
    incoming order was a sell that hit the bid."""
    maker_buyer = TRADE.replace('"m":false', '"m":true')
    assert parse_trade(maker_buyer).aggressor is Side.SELL


def test_taker_buyer_is_the_aggressor() -> None:
    """`m` false: the buyer crossed the spread to lift the offer.

    The pair of these two tests is the only thing standing between Phase 4 and
    a sign-flipped order-flow imbalance that looks entirely plausible.
    """
    assert parse_trade(TRADE).aggressor is Side.BUY


def test_binance_maker_flag_does_not_survive_the_adapter() -> None:
    """`m` is a Binance word. Downstream sees a `Side` or nothing."""
    assert not hasattr(parse_trade(TRADE), "m")
    assert not hasattr(parse_trade(TRADE), "is_buyer_maker")


def test_rejects_non_trade_frame() -> None:
    with pytest.raises(ValueError, match="trade"):
        parse_trade(FRAME)


# --- combined stream --------------------------------------------------------

DEPTH_ENVELOPE = f'{{"stream":"btcusdt@depth","data":{FRAME}}}'
TRADE_ENVELOPE = f'{{"stream":"btcusdt@trade","data":{TRADE}}}'


def test_combined_frame_dispatches_on_event_type() -> None:
    """One socket carries both subscriptions, so the envelope is unwrapped and
    the body decides the type."""
    assert isinstance(parse_stream_frame(DEPTH_ENVELOPE), BookUpdate)
    assert isinstance(parse_stream_frame(TRADE_ENVELOPE), Trade)


def test_combined_frame_preserves_received_time() -> None:
    """Unwrapping must not restart the clock."""
    assert parse_stream_frame(TRADE_ENVELOPE, RECEIVED_NS).received_ns == RECEIVED_NS


def test_bare_frame_is_not_a_combined_frame() -> None:
    """A single-stream frame has no envelope. Accepting it would mean silently
    parsing whatever `data` happened to be absent from."""
    with pytest.raises(ValueError, match="combined-stream"):
        parse_stream_frame(TRADE)


def test_unhandled_event_type_is_rejected() -> None:
    """Binance multiplexes many stream types onto this endpoint; subscribing
    to one this adapter cannot translate must be loud, not silently dropped."""
    with pytest.raises(ValueError, match="kline"):
        parse_stream_frame('{"stream":"btcusdt@kline_1m","data":{"e":"kline"}}')
