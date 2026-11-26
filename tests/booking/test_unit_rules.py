"""Pure unit tests (no MySQL/Kafka): request validation, idempotency hash, retry helper."""
from datetime import date

import pymysql
import pytest

from booking_core import validation
from booking_core.db import is_transient
from booking_core.errors import ApiError
from booking_worker.consumer_utils import BACKOFF_S, RetryExhausted, run_with_retry

TODAY = date(2026, 11, 24)
GOOD = {"property_id": "property-2001", "check_in": "2026-12-01", "check_out": "2026-12-03", "guest_count": 2}


@pytest.mark.parametrize("patch,field", [
    ({"property_id": None}, "property_id"),
    ({"check_in": "2026/12/01"}, "check_in"),
    ({"check_in": "2026-02-30"}, "check_in"),
    ({"check_out": "2026-12-01"}, "check_out"),            # == check_in
    ({"check_in": "2026-11-23"}, "check_in"),              # in the past
    ({"check_out": "2027-01-01"}, "check_out"),            # 31 nights
    ({"guest_count": 0}, "guest_count"),
    ({"guest_count": "2"}, "guest_count"),
    ({"guest_count": True}, "guest_count"),
    ({"message_to_host": "x" * 1001}, "message_to_host"),
])
def test_create_validation_errors(patch, field):
    with pytest.raises(ApiError) as e:
        validation.create_request({**GOOD, **patch}, TODAY)
    assert e.value.status == 400 and e.value.code == "VALIDATION_ERROR"
    assert e.value.details["field"] == field


def test_create_validation_accepts_30_nights_and_today():
    r = validation.create_request({**GOOD, "check_in": "2026-11-24", "check_out": "2026-12-24"}, TODAY)
    assert r.nights == 30


def test_request_hash_is_key_order_independent():
    assert validation.request_hash({"a": 1, "b": 2}) == validation.request_hash({"b": 2, "a": 1})
    assert validation.request_hash({"a": 1}) != validation.request_hash({"a": 2})


def test_retry_backoff_200ms_1s_5s_then_exhausted():
    sleeps, calls = [], []

    def fail():
        calls.append(1)
        raise pymysql.err.OperationalError(1213, "deadlock")
    with pytest.raises(RetryExhausted) as e:
        run_with_retry(fail, is_transient=is_transient, sleep=sleeps.append)
    assert sleeps == list(BACKOFF_S) == [0.2, 1.0, 5.0]
    assert len(calls) == 4 and e.value.retries == 3


def test_retry_succeeds_and_non_transient_not_retried():
    state = {"n": 0}

    def flaky():
        state["n"] += 1
        if state["n"] < 3:
            raise pymysql.err.OperationalError(1205, "lock wait timeout")
        return "ok"
    assert run_with_retry(flaky, is_transient=is_transient, sleep=lambda s: None) == ("ok", 2)
    with pytest.raises(ValueError):
        run_with_retry(lambda: (_ for _ in ()).throw(ValueError("bad")), is_transient=is_transient,
                       sleep=lambda s: None)


def test_api_transaction_retry_on_deadlock_only():
    from booking_core.db import retry_transaction
    calls, sleeps = [], []

    def deadlock_twice():
        calls.append(1)
        if len(calls) <= 2:
            raise pymysql.err.OperationalError(1213, "deadlock")
        return "committed"
    assert retry_transaction(deadlock_twice, sleep=sleeps.append) == "committed" and sleeps == [0.05, 0.2]
    with pytest.raises(pymysql.err.OperationalError):
        retry_transaction(lambda: (_ for _ in ()).throw(pymysql.err.OperationalError(2003, "down")),
                          sleep=lambda s: None)


def test_ids_up_to_64_chars_match_role_a_schema():
    long_id = "property-" + "9" * 55                       # 64 characters, the VARCHAR(64) limit
    assert validation.create_request({**GOOD, "property_id": long_id}, TODAY).property_id == long_id
    with pytest.raises(ApiError):
        validation.create_request({**GOOD, "property_id": long_id + "9"}, TODAY)
