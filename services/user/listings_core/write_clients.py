import httpx

from .http import ApiError, service_token, unavailable


class WriteDependencies:
    def __init__(self, settings, *, transport=None):
        self.settings, self.transport = settings, transport

    def _get(self, url, trace_id):
        try:
            with httpx.Client(timeout=self.settings.timeout_s, transport=self.transport,
                              follow_redirects=False) as client:
                response = client.get(url, headers={"Authorization": "Bearer " + service_token(self.settings),
                                                    "X-Trace-Id": trace_id})
                response.raise_for_status()
                body = response.json()
                if not isinstance(body, dict):
                    raise ValueError("Expected an object")
                return body
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            raise unavailable("Property write validation dependency") from None

    def require_host(self, host_id, trace_id):
        # Existing User contract uses POST /hosts/get, with a service JWT.
        try:
            with httpx.Client(timeout=self.settings.timeout_s, transport=self.transport,
                              follow_redirects=False) as client:
                response = client.post(self.settings.user_url + "/hosts/get", json={"host_id": host_id},
                                       headers={"Authorization": "Bearer " + service_token(self.settings),
                                                "X-Trace-Id": trace_id})
                if response.status_code == 404:
                    raise ApiError(404, "HOST_NOT_FOUND", "A host profile is required.")
                response.raise_for_status()
                body = response.json()
                if body["host_id"] != host_id:
                    raise ValueError("Host identity mismatch")
        except (httpx.HTTPError, ValueError, KeyError, TypeError):
            raise unavailable("User host validation") from None

    def require_no_active_bookings(self, property_id, trace_id):
        body = self._get(self.settings.booking_url + f"/internal/properties/{property_id}/active-reservations", trace_id)
        count, active = body.get("count"), body.get("has_active_reservations")
        if body.get("property_id") != property_id or type(active) is not bool or type(count) is not int or count < 0 or active != (count > 0):
            raise unavailable("Booking reservation validation")
        if active:
            raise ApiError(409, "ACTIVE_RESERVATION_EXISTS", "Active reservations prevent archiving.")
