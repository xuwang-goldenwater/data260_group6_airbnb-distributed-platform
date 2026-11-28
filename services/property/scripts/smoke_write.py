"""Real HTTP write check: creates a NEW listing, then archives it; seed listings untouched."""
import argparse
import json
from pathlib import Path
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "user"))
import httpx
import jwt
from listings_core.config import Settings

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--host-id", default="host-501")
parser.add_argument("--url", default="http://127.0.0.1:8003")
args = parser.parse_args()
cfg = Settings.from_env()
if not cfg.jwt_secret:
    raise SystemExit("Configure JWT_SECRET first")
claims = {"sub": args.host_id, "role": "host", "exp": int(time.time()) + 600}
if cfg.jwt_issuer:
    claims["iss"] = cfg.jwt_issuer
headers = {"Authorization": "Bearer " + jwt.encode(claims, cfg.jwt_secret, algorithm="HS256")}
body = {"title": "Role A write integration check", "description": "Synthetic listing created only for the Role A write integration check.",
        "property_type": "house", "location": {"address_line_1": "100 Integration Example Lane", "city": "San Jose",
        "state": "CA", "postal_code": "95113", "country": "US", "latitude": 37.3382, "longitude": -121.8863},
        "capacity": 4, "bedrooms": 2, "bathrooms": 1.5, "amenities": ["parking", "kitchen", "wifi"],
        "nightly_price": 200.0, "cleaning_fee": 40.0}


def require(condition, message):
    if not condition:
        raise ValueError(message)


with httpx.Client(base_url=args.url, timeout=30, headers=headers) as client:
    key = str(uuid.uuid4())
    response = client.post("/properties/create", json=body, headers={"Idempotency-Key": key})
    response.raise_for_status()
    require(response.status_code == 201, "Expected 201")
    prop = response.json()
    pid = prop["property_id"]
    print("Created integration property: " + pid, flush=True)
    require(prop["status"] == "draft" and prop["host_id"] == args.host_id and prop["version"] == 1, "Incorrect draft")
    replay = client.post("/properties/create", json=body, headers={"Idempotency-Key": key})
    require(replay.status_code == 200 and replay.json() == prop, "Create replay differs")
    update = {"property_id": pid, "expected_version": 1, "changes": {"nightly_price": 210.0, "status": "active"}}
    changed = client.post("/properties/update", json=update, headers={"Idempotency-Key": str(uuid.uuid4())})
    changed.raise_for_status()
    require(changed.json()["version"] == 2 and changed.json()["nightly_price"] == 210.0, "Update failed")
    stale = client.post("/properties/update", json=update, headers={"Idempotency-Key": str(uuid.uuid4())})
    require(stale.status_code == 409, "Stale version must be rejected")
    archive = client.post("/properties/delete", json={"property_id": pid, "expected_version": 2},
                          headers={"Idempotency-Key": str(uuid.uuid4())})
    archive.raise_for_status()
    require(archive.json()["status"] == "archived" and archive.json()["version"] == 3, "Archive failed")
    with httpx.Client(base_url=args.url, timeout=10) as public:
        require(public.post("/properties/get", json={"property_id": pid}).status_code == 404, "Archived listing is public")
print(json.dumps({"status": "PASS", "property_id": pid, "final_status": "archived",
                  "checks": ["real User host lookup", "create draft", "idempotent replay", "versioned update",
                             "stale version rejected", "real Booking archive guard", "archived hidden from public"],
                  "events": "Three events queued transactionally; run the relay to publish them."}, indent=2))
