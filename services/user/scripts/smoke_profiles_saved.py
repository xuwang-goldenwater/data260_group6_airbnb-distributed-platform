"""Real HTTP profile/save check. Creates new synthetic profiles, never edits seed profiles."""
from datetime import datetime, timezone
import argparse
import json
from pathlib import Path
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import httpx
import jwt
from listings_core.config import Settings
from listings_core.repository import MySQLRepository

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--user-url", default="http://127.0.0.1:8001")
parser.add_argument("--property-url", default="http://127.0.0.1:8003")
parser.add_argument("--property-id", default="property-2001")
args = parser.parse_args()
cfg = Settings.from_env()
if not cfg.jwt_secret:
    raise SystemExit("Configure JWT_SECRET first")


def auth(subject, role):
    claims = {"sub": subject, "role": role, "exp": int(time.time()) + 600}
    if cfg.jwt_issuer:
        claims["iss"] = cfg.jwt_issuer
    return {"Authorization": "Bearer " + jwt.encode(claims, cfg.jwt_secret, algorithm="HS256")}


def require(condition, message):
    if not condition:
        raise ValueError(message)


with httpx.Client(timeout=30) as client:
    for url in (args.user_url, args.property_url):
        client.get(url + "/ready").raise_for_status()
    client.post(args.property_url + "/properties/get", json={"property_id": args.property_id}).raise_for_status()
    gid, hid = "guest-integration-" + uuid.uuid4().hex, "host-integration-" + uuid.uuid4().hex
    email = gid + "@example.com"
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    repo = MySQLRepository(cfg)
    # Explicit test-fixture setup, not public registration. Both records are NEW.
    with repo.connection(write=True) as conn, conn.cursor() as cur:
        cur.execute("INSERT INTO guests (guest_id,first_name,last_name,email,preferences,saved_properties_count,booking_count,profile_created_datetime,updated_datetime) VALUES (%s,%s,%s,%s,%s,0,0,%s,%s)",
                    (gid,"Integration","Guest",email,'{}',now,now))
        cur.execute("INSERT INTO hosts (host_id,name,email,verification_status,response_rate,host_since,access_level,updated_datetime) VALUES (%s,%s,%s,%s,0,%s,%s,%s)",
                    (hid,"Integration Host",hid+"@example.com","pending",now.date(),"host",now))
    print(json.dumps({"created_fixture_guest": gid, "created_fixture_host": hid}), flush=True)
    guest, host, service = auth(gid,"guest"), auth(hid,"host"), auth("profile-smoke","service")

    def post(url, body, headers, key=None):
        return client.post(url, json=body, headers={**headers,"Idempotency-Key":key or str(uuid.uuid4())})

    key = str(uuid.uuid4())
    update = {"guest_id":gid,"phone":"+14085550199","location":"San Jose, CA","preferences":{"max_nightly_price":180}}
    r = post(args.user_url+"/guests/update",update,guest,key)
    r.raise_for_status()
    require(r.json()["phone"]=="+14085550199", "Guest update failed")
    replay = post(args.user_url+"/guests/update",update,guest,key)
    require(replay.status_code==200 and replay.json()==r.json(), "Profile replay differs")
    r = post(args.user_url+"/hosts/update",{"host_id":hid,"name":"Updated Integration Host"},host)
    r.raise_for_status()
    require(r.json()["name"]=="Updated Integration Host", "Host update failed")
    save = {"property_id":args.property_id,"saved":True}
    r = post(args.property_url+"/properties/save",save,guest)
    r.raise_for_status()
    require(r.json()["saved_properties_count"]==1, "Save count must be one")
    r = post(args.property_url+"/properties/save",save,guest)
    r.raise_for_status()
    require(r.json()["saved_properties_count"]==1, "Same-state save created a duplicate")
    r = post(args.user_url+"/guests/get",{"guest_id":gid},guest)
    r.raise_for_status()
    require(r.json()["saved_properties_count"]==1, "User counter differs")
    r = post(args.property_url+"/properties/save",{**save,"saved":False},guest)
    r.raise_for_status()
    require(r.json()["saved_properties_count"]==0, "Unsave count must be zero")
    r = post(args.user_url+"/guests/search",{"email":email},service)
    r.raise_for_status()
    require(r.json()["total"]==1 and "email" not in r.json()["items"][0], "Guest search privacy check failed")
    r = post(args.user_url+"/guests/delete",{"guest_id":gid},guest)
    r.raise_for_status()
    require(r.json()["deleted"] is True, "Soft delete failed")
    require(client.get(args.user_url+"/internal/guests/"+gid,headers=service).status_code==404, "Deleted guest remains active")
    with repo.connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS n FROM property_event_outbox WHERE topic='property.saved' AND JSON_UNQUOTE(JSON_EXTRACT(envelope,'$.payload.guest_id'))=%s",(gid,))
        require(cur.fetchone()["n"]==2, "Expected exactly two saved-state events")
print(json.dumps({"status":"PASS","guest_id":gid,"host_id":hid,"guest_final_status":"soft_deleted",
                  "checks":["guest/host profile updates","profile replay","save and same-state no-op","User saved count",
                            "unsave","restricted guest search","real Booking guest deletion guard","deleted guest hidden"],
                  "events":"Two property.saved events queued; verify relay acknowledgement separately."},indent=2))
