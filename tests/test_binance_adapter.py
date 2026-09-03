"""Binance adapter translates the wire format without losing information.

The sample below is a real captured frame, trimmed to a few levels. It is
embedded rather than read from data/ because captures are gitignored.
"""

from decimal import Decimal

import pytest

from tickforge.adapters.binance import parse_depth_update, parse_snapshot

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
