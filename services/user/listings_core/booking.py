from datetime import datetime

import httpx

from .http import service_token, unavailable


class BookingClient:
    def __init__(self, settings, *, transport=None):
        self.settings, self.transport = settings, transport

    def check(self, property_ids, check_in, check_out, trace_id):
        if not property_ids:
            return {}, None
        result, checked = {}, None
        headers = {"Authorization": "Bearer " + service_token(self.settings), "X-Trace-Id": trace_id}
        try:
            with httpx.Client(base_url=self.settings.booking_url, timeout=self.settings.timeout_s,
                              transport=self.transport, follow_redirects=False) as client:
                for start in range(0, len(property_ids), 200):
                    batch = property_ids[start:start + 200]
                    response = client.post("/internal/availability/check", headers=headers,
                                           json={"property_ids": batch, "check_in": check_in.isoformat(),
                                                 "check_out": check_out.isoformat()})
                    response.raise_for_status()
                    payload = response.json()
                    if payload["check_in"] != check_in.isoformat() or payload["check_out"] != check_out.isoformat():
                        raise ValueError("Booking returned a different date range")
                    checked = payload["checked_datetime"]
                    timestamp = datetime.fromisoformat(checked.replace("Z", "+00:00"))
                    if timestamp.tzinfo is None:
                        raise ValueError("Booking timestamp lacks a UTC offset")
                    rows = payload["results"]
                    if not isinstance(rows, list) or len(rows) != len(batch):
                        raise ValueError("Incomplete Booking response")
                    found = {}
                    for row in rows:
                        pid = row["property_id"]
                        if pid in found or type(row["available"]) is not bool:
                            raise ValueError("Invalid or duplicate Booking result")
                        found[pid] = row["available"]
                    if set(found) != set(batch):
                        raise ValueError("Booking returned the wrong property IDs")
                    result.update(found)
        except (httpx.HTTPError, ValueError, KeyError, TypeError, AttributeError):
            raise unavailable("Booking availability") from None
        return result, checked
