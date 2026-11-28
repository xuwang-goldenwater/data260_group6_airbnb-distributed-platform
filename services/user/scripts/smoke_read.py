"""Check running Role A HTTP APIs. Add --booking to check live Booking availability."""
import argparse
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx
import jwt
from listings_core.config import Settings

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--user-url", default="http://127.0.0.1:8001")
parser.add_argument("--property-url", default="http://127.0.0.1:8003")
parser.add_argument("--booking", action="store_true")
args = parser.parse_args()
cfg = Settings.from_env()
if not cfg.jwt_secret:
    raise SystemExit("Configure JWT_SECRET first")
claims = {"sub": "role-a-smoke", "role": "service", "exp": int(time.time()) + 300}
if cfg.jwt_issuer:
    claims["iss"] = cfg.jwt_issuer
headers = {"Authorization": "Bearer " + jwt.encode(claims, cfg.jwt_secret, algorithm="HS256")}
checks = []


def require(condition, message):
    if not condition:
        raise ValueError(message)


try:
    with httpx.Client(timeout=30) as client:
        for base, service in ((args.user_url, "user"), (args.property_url, "property")):
            response = client.get(base + "/ready")
            response.raise_for_status()
            checks.append(service + " MySQL readiness")
        response = client.post(args.user_url + "/guests/get", headers=headers, json={"guest_id": "guest-1001"})
        response.raise_for_status()
        require(response.json()["guest_id"] == "guest-1001", "Guest mismatch")
        checks.append("authenticated guest read")
        response = client.get(args.user_url + "/internal/guests/guest-1001", headers=headers)
        response.raise_for_status()
        require(response.json()["active"] is True, "Guest must be active")
        checks.append("internal guest validation")
        response = client.post(args.property_url + "/properties/get", json={"property_id": "property-2007"})
        response.raise_for_status()
        require(response.json()["rating"] == 4.3, "Expected seed rating 4.3")
        checks.append("property MySQL read")
        response = client.post(args.property_url + "/properties/search", json={"location": "San Jose",
                               "guest_count": 2, "maximum_nightly_price": 250,
                               "required_amenities": ["parking", "kitchen"], "page_size": 50})
        response.raise_for_status()
        require(response.json()["total"] > 0, "Expected search results")
        checks.append("search with capacity, price and all-amenity filters")
        if args.booking:
            manifest = json.loads((cfg.seed_dir / "manifest.json").read_text())
            sc = manifest["scenarios"]
            response = client.post(args.property_url + "/properties/availability", json={"property_id": "property-2001",
                                   "check_in": sc["check_in"], "check_out": sc["check_out"], "guest_count": 2})
            response.raise_for_status()
            require(response.json()["source"] == "booking_service" and type(response.json()["available"]) is bool,
                    "Expected a Booking-service availability result")
            checks.append("Property HTTP -> Booking HTTP -> availability")
    print(json.dumps({"status": "PASS", "checks": checks}, indent=2))
except (httpx.HTTPError, ValueError, KeyError, TypeError) as error:
    raise SystemExit("Smoke check failed: " + str(error))
