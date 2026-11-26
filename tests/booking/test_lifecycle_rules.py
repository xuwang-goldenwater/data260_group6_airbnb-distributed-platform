"""Pure unit tests (no MySQL/Kafka): status transitions and the cancellation policy."""
from datetime import date

import pytest

from booking_core import cancellation_policy
from booking_core.errors import ApiError
from booking_core.models import ALLOWED_TRANSITIONS, STATUSES, assert_transition, is_allowed

ALLOWED = {("requested", "pending_host"), ("requested", "rejected"), ("requested", "cancelled"),
           ("pending_host", "confirmed"), ("pending_host", "declined"), ("pending_host", "cancelled"),
           ("confirmed", "completed"), ("confirmed", "cancelled")}


@pytest.mark.parametrize("src", STATUSES)
@pytest.mark.parametrize("dst", STATUSES)
def test_every_transition_pair(src, dst):
    assert is_allowed(src, dst) == ((src, dst) in ALLOWED)
    if (src, dst) in ALLOWED:
        assert_transition(src, dst, "res-x")
    else:
        with pytest.raises(ApiError) as e:
            assert_transition(src, dst, "res-x")
        assert e.value.status == 409 and e.value.code == "INVALID_STATUS_TRANSITION"


def test_transition_table_has_exactly_the_documented_arrows():
    assert {(s, d) for s, ds in ALLOWED_TRANSITIONS.items() for d in ds} == ALLOWED


def _res(status, check_in):
    return {"status": status, "check_in": check_in}


@pytest.mark.parametrize("status,check_in,today,allowed", [
    ("confirmed", date(2026, 12, 10), date(2026, 12, 9), True),
    ("confirmed", date(2026, 12, 10), date(2026, 12, 10), False),   # on check-in day: closed
    ("confirmed", date(2026, 12, 10), date(2026, 12, 11), False),
    ("pending_host", date(2026, 12, 10), date(2026, 12, 10), True),
    ("requested", date(2026, 12, 10), date(2026, 12, 10), True),
])
def test_cancellation_window(status, check_in, today, allowed):
    d = cancellation_policy.evaluate(_res(status, check_in), "guest", today)
    assert d.allowed is allowed
    if not allowed:
        assert d.code == "CANCELLATION_WINDOW_CLOSED"


def test_cancel_reason_codes():
    assert cancellation_policy.status_reason("guest") == "GUEST_CANCELLED"
    assert cancellation_policy.status_reason("host") == "HOST_CANCELLED"
