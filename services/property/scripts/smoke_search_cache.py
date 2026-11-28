"""Live search HTTP/Redis/Booking plus synthetic Kafka invalidation hint."""
from datetime import date, timedelta
import hashlib
import json
from pathlib import Path
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "services/user")]
import argparse
import httpx
import jwt
from listings_core.config import Settings
from listings_core.search_cache import make_client, prefix
from listings_core.rating_worker import AcknowledgedPublisher
from shared.consumer import build


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--property-url", default="http://127.0.0.1:8003")
    args = parser.parse_args()
    cfg = Settings.from_env()
    redis = make_client(cfg)
    claims = {"sub": "cache-smoke-service", "role": "service", "exp": int(time.time()) + 600}
    if cfg.jwt_issuer: claims["iss"] = cfg.jwt_issuer
    if not cfg.jwt_secret: raise SystemExit("Configure the existing shared JWT_SECRET first")
    headers = {"Authorization": "Bearer " + jwt.encode(claims, cfg.jwt_secret, algorithm="HS256")}
    manifest = json.loads((cfg.seed_dir / "manifest.json").read_text())
    ci, co = manifest["scenarios"]["check_in"], manifest["scenarios"]["check_out"]
    trace = "tr-cache-smoke-" + uuid.uuid4().hex
    body = {"location": "San Jose", "guest_count": 2, "maximum_nightly_price": 250.0,
            "minimum_nightly_price": (int(uuid.uuid4().hex[:12], 16) % 1000000) / 1000000,
            "required_amenities": ["parking", "kitchen"], "page_size": 50}
    def require(ok, message):
        if not ok: raise ValueError(message)
    with httpx.Client(base_url=args.property_url, timeout=20) as client:
        stats = client.get("/internal/cache/stats", headers=headers); stats.raise_for_status()
        require(stats.json()["enabled"] and stats.json()["redis_available"], "Start Property with PROPERTY_CACHE_ENABLED=true and matching Redis config")
        def search(filters):
            r = client.post("/properties/search", json=filters, headers={"X-Trace-Id": trace})
            r.raise_for_status()
            return r
        first, warm = search(body), search(body)
        require(first.headers.get("x-search-cache") == "MISS" and warm.headers.get("x-search-cache") == "HIT", "Expected cold MISS then HIT")
        require(first.json() == warm.json(), "Cached and uncached listing results differ")
        dated = {**body, "check_in": ci, "check_out": co, "available_only": True}
        search(dated)
        dated_hit = search(dated)
        require(dated_hit.headers.get("x-search-cache") == "HIT", "Dated listing data did not hit cache")
        result = dated_hit.json()
        require(result["availability_checked_datetime"] is not None and all(p["available"] is True for p in result["items"]), "No live availability evidence")
        ids = [p["property_id"] for p in result["items"]]
        require("property-2002" not in ids and "property-2001" in ids, "Seed demo availability differs; inspect live bookings before interpreting this check")
        rid = "res-cache-hint-" + uuid.uuid4().hex
        nights = [(date.fromisoformat(ci) + timedelta(days=i)).isoformat() for i in range((date.fromisoformat(co)-date.fromisoformat(ci)).days)]
        event = build("property.availability.updated", "cache-smoke", "property", "property-2001",
                      {"property_id": "property-2001", "change": "held", "nights": nights, "reservation_id": rid},
                      "availability:" + rid + ":held", trace)
        event["producer"] = "property-cache-synthetic-hint"
        AcknowledgedPublisher(cfg.kafka_bootstrap)("property.availability.updated", "property-2001", event)
        receipt = prefix(cfg) + "receipt:" + hashlib.sha256(event["idempotency_key"].encode()).hexdigest()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and redis.get(receipt) is None:
            time.sleep(.5)
        require(redis.get(receipt) is not None, "Cache event receipt missing; inspect invalidator terminal/config/DLQ")
        invalidated = search(body)
        require(invalidated.headers.get("x-search-cache") == "MISS", "Expected MISS after Kafka namespace invalidation")
        require(invalidated.json() == first.json(), "An invalidation hint must not change listing/booking data")
    print(json.dumps({"status": "PASS", "trace_id": trace,
                      "checks": ["Redis MISS then HIT", "identical listing results", "dated cache HIT rechecks live Booking",
                                 "occupied demo listing excluded", "Kafka cache-event receipt", "MISS after invalidation"],
                      "scope": "Synthetic invalidation hint; no reservation or listing was changed"}, indent=2))


if __name__ == "__main__":
    main()
