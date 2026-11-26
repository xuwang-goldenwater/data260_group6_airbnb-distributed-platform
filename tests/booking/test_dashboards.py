"""Integration: guest and host dashboards."""
from __future__ import annotations

from test_api_flow import GUEST, HOST, OTHER, _act, held_booking


def test_dashboards(client, auth, worker, producer):
    res = held_booking(client, auth, worker, producer)
    _act(client, auth, "approve", HOST, res)
    other = held_booking(client, auth, worker, producer, ci=20, co=22, sub=OTHER)
    _act(client, auth, "decline", HOST, other)
    g = client.post("/dashboards/guest", json={"days": 30}, headers=auth(GUEST, key=None)).json()
    assert g["bookings_in_window"] == 1 and g["status_summary"]["confirmed"] == 1 and g["total_spent"] == 400.0
    assert g["upcoming"][0]["reservation_id"] == res["reservation_id"] and g["saved_properties_count"] == 3
    h = client.post("/dashboards/host", json={"days": 30}, headers=auth(HOST, key=None)).json()
    assert h["funnel"] == {"views": None, "saves": None, "requests": 2, "confirmed": 1}
    assert h["declined"] == 1 and h["revenue"] == 400.0 and h["median_response_minutes"] == 0
    assert h["cancellations"] == {"by_guest": 0, "by_host": 0} and h["average_rating"] is not None
    r = client.post("/dashboards/host", json={"property_id": "property-2007"}, headers=auth(HOST, key=None))
    assert r.status_code == 403
    r = client.post("/dashboards/guest", json={"days": 0}, headers=auth(GUEST, key=None))
    assert r.status_code == 400
