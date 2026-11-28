"""Real HTTP -> view outbox check, optionally waiting for the existing relay's Kafka ack."""
import argparse
import json
from pathlib import Path
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "services/user")]
import httpx
import jwt
from listings_core.config import Settings
from listings_core.repository import MySQLRepository
from shared.consumer import validate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--property-url", default="http://127.0.0.1:8003")
    parser.add_argument("--property-id", default="property-2001")
    parser.add_argument("--guest-id", default="guest-1001")
    parser.add_argument("--wait-published", action="store_true")
    args = parser.parse_args()
    cfg, trace = Settings.from_env(), "tr-view-smoke-" + uuid.uuid4().hex
    if not cfg.jwt_secret: raise SystemExit("Configure JWT_SECRET first")
    repo = MySQLRepository(cfg)
    before = repo.property(args.property_id)
    if before is None or before["status"] != "active": raise SystemExit("Choose an existing active property")
    def auth(subject, role):
        claims = {"sub": subject, "role": role, "exp": int(time.time()) + 600}
        if cfg.jwt_issuer: claims["iss"] = cfg.jwt_issuer
        return {"Authorization": "Bearer " + jwt.encode(claims, cfg.jwt_secret, algorithm="HS256")}
    guest = auth(args.guest_id, "guest")
    def require(ok, message):
        if not ok: raise ValueError(message)
    def rows():
        with repo.connection() as conn, conn.cursor() as cursor:
            cursor.execute("SELECT envelope,published_datetime FROM property_event_outbox WHERE topic='property.viewed' "
                           "AND JSON_UNQUOTE(JSON_EXTRACT(envelope,'$.trace_id'))=%s", (trace,))
            return list(cursor.fetchall())
    with httpx.Client(timeout=15) as client:
        client.get(args.property_url + "/ready").raise_for_status()
        def get(headers, key=None, pid=None):
            headers = {**headers, "X-Trace-Id": trace}
            if key is not None: headers["Idempotency-Key"] = key
            return client.post(args.property_url + "/properties/get", json={"property_id": pid or args.property_id}, headers=headers)
        key = str(uuid.uuid4())
        get({}).raise_for_status()
        get(auth(before["host_id"], "host")).raise_for_status()
        get(auth("view-smoke-service", "service")).raise_for_status()
        first = get(guest, key); first.raise_for_status()
        replay = get(guest, key); replay.raise_for_status()
        require(replay.headers.get("idempotent-replay") == "true" and replay.json() == first.json(), "View replay failed")
        second = get(guest, str(uuid.uuid4())); second.raise_for_status()
        require(get(guest, pid="property-view-missing-" + uuid.uuid4().hex).status_code == 404, "Missing property must be 404")
        client.post(args.property_url + "/properties/search", json={"location": "San Jose"}, headers={**guest, "X-Trace-Id": trace}).raise_for_status()
    records = rows()
    require(len(records) == 2, "Expected exactly two new view events")
    for row in records:
        event = json.loads(row["envelope"]) if isinstance(row["envelope"], str) else row["envelope"]
        validate(event, key=args.property_id, check_key=True)
        require(event["actor_id"] == args.guest_id and event["payload"]["guest_id"] == args.guest_id, "Guest identity differs")
        require(set(event["payload"]) == {"property_id", "guest_id", "viewed_datetime"}, "Unexpected view payload fields")
    require(repo.property(args.property_id) == before, "Viewing must not modify listing fields")
    if args.wait_published:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and not all(r["published_datetime"] is not None for r in records):
            time.sleep(.5)
            records = rows()
        require(all(r["published_datetime"] is not None for r in records), "Views queued but relay acknowledgement timed out; check relay terminal; do not rerun just to publish")
    print(json.dumps({"status": "PASS", "trace_id": trace, "property_id": args.property_id,
                      "queued": len(records), "published": sum(r["published_datetime"] is not None for r in records),
                      "checks": ["real authenticated detail reads", "same-key retry emits once", "new key creates new view",
                                 "anonymous/host/service/search/404 emit no view", "JWT guest identity and shared envelope",
                                 "listing fields unchanged"],
                      "scope": "HTTP/outbox and optional relay Kafka acknowledgement; Analytics ingestion not checked"}, indent=2))


if __name__ == "__main__":
    main()
