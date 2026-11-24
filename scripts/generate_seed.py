#!/usr/bin/env python3
"""Generate and validate synthetic API-shaped fixtures; Python 3.9+, stdlib only."""
import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import random

STATUSES = {"requested", "pending_host", "confirmed", "rejected", "declined", "cancelled", "completed"}
HELD = {"pending_host", "confirmed", "completed"}  # retain completed nights as history
TRANSITIONS = {"requested": [None], "pending_host": [None, "requested"],
               "rejected": [None, "requested"], "confirmed": [None, "requested", "pending_host"],
               "declined": [None, "requested", "pending_host"],
               "cancelled": [None, "requested", "pending_host", "confirmed"],
               "completed": [None, "requested", "pending_host", "confirmed"]}
CITIES = [("San Jose", 37.3382, -121.8863, "95113"),
          ("Santa Cruz", 36.9741, -122.0308, "95060"),
          ("Monterey", 36.6002, -121.8947, "93940"),
          ("San Francisco", 37.7749, -122.4194, "94103")]
TYPES = ["apartment", "house", "condo", "cabin", "room"]
TEXTS = {"positive": "Clean and comfortable stay. The host replied quickly and parking was easy.",
         "cleanliness": "Cleanliness was disappointing: dirty bedding and dust in the room.",
         "missing_amenity": "The listed kitchen was missing an advertised cooking appliance.",
         "noise": "Noise from the street made it difficult to sleep during our stay.",
         "cancellation": "The host threatened a cancellation before arrival, but our stay proceeded.",
         "host_unresponsive": "The host was unresponsive when we requested check-in instructions.",
         "price_mismatch": "An unexpected fee caused a price mismatch with the listing."}


def stamp(day, hour=12):
    return datetime.combine(day, time(hour), timezone.utc).isoformat().replace("+00:00", "Z")


def opaque(prefix, n):
    # Valid deterministic ULID encoding (synthetic timestamp/randomness, not real creation time).
    alphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
    value = n
    result = ""
    for _ in range(26):
        result = alphabet[value % 32] + result
        value //= 32
    return prefix + result


def price_total(price, nights, fee):
    return float(Decimal(str(price)) * nights + Decimal(str(fee)))


def build(seed, anchor):
    rng = random.Random(seed)
    old = anchor - timedelta(days=900)
    initial = stamp(old)
    guests = [dict(guest_id=f"guest-{1001+i}", first_name=f"Demo{i+1}", last_name="Guest",
                   email=f"guest{i+1}@example.com", phone=None, location="San Jose, CA",
                   profile_photo_url=None, preferences={"property_types": ["apartment", "condo"],
                   "required_amenities": ["parking", "kitchen"], "max_nightly_price": 250.0},
                   saved_properties_count=0, booking_count=0,
                   profile_created_datetime=initial, updated_datetime=stamp(anchor)) for i in range(120)]
    hosts = [dict(host_id=f"host-{501+i}", name=f"Demo Host {i+1}",
                  email=f"host{i+1}@example.com", phone=None, profile_photo_url=None,
                  verification_status="verified", response_rate=0.0, response_time=None,
                  host_since=old.isoformat(), access_level="host", updated_datetime=stamp(anchor))
             for i in range(60)]
    props = []
    for i in range(200):
        city, lat, lon, postal = CITIES[i % len(CITIES)]
        props.append(dict(property_id=f"property-{2001+i}", host_id=hosts[i % 60]["host_id"],
                          title=f"Synthetic {city} {TYPES[i % 5]} {i+1}",
                          description="Fictional demonstration listing with synthetic address and amenities. Not a real rental.",
                          property_type=TYPES[i % 5], location=dict(address_line_1=f"{100+i} Example Lane",
                          city=city, state="CA", postal_code=postal, country="US",
                          latitude=round(lat+rng.uniform(-0.02, 0.02), 6),
                          longitude=round(lon+rng.uniform(-0.02, 0.02), 6)),
                          capacity=rng.choice([2, 4, 6, 8]), bedrooms=rng.randint(1, 4), bathrooms=1.5,
                          amenities=sorted(set(["wifi"]+rng.sample(["parking", "kitchen", "pool", "washer"], 3))),
                          nightly_price=float(rng.randrange(80, 401, 5)), cleaning_fee=40.0,
                          availability="available", status="active", rating=0.0, review_count=0,
                          version=1, created_datetime=initial, updated_datetime=stamp(anchor)))
    # Controlled, comparable candidates; labels are fixture intentions, not evaluated AI results.
    for i in [0, 6, 11, 43]:
        props[i].update(capacity=4, nightly_price=180.0, amenities=["kitchen", "parking", "wifi"])
        props[i]["location"].update(city="San Jose", latitude=37.3382, longitude=-121.8863, postal_code="95113")
    # Non-active properties exercise search exclusion. No reservations reference these.
    for i, status in [(197, "draft"), (198, "suspended"), (199, "archived")]:
        props[i]["status"] = status
    reservations, history, blocks, reviews = [], [], [], []

    def reserve(p, status, start, nights=2):
        n = len(reservations)+1
        guest = guests[(n-1) % len(guests)]
        end = start+timedelta(days=nights)
        created = start-timedelta(days=10) if status == "completed" else anchor-timedelta(days=2)
        path = ["requested"] + [x for x in TRANSITIONS[status] if x not in {None, "requested"}]
        if status != "requested":
            path.append(status)
        r = dict(reservation_id=opaque("res-", n), property_id=p["property_id"],
                 guest_id=guest["guest_id"], host_id=p["host_id"], check_in=start.isoformat(),
                 check_out=end.isoformat(), nights=nights, guest_count=2,
                 nightly_price=p["nightly_price"], cleaning_fee=p["cleaning_fee"],
                 total_price=price_total(p["nightly_price"], nights, p["cleaning_fee"]), status=status,
                 status_reason={"rejected": "DATES_UNAVAILABLE", "declined": "HOST_DECLINED",
                                "cancelled": "GUEST_CANCELLED"}.get(status),
                 message_to_host="Synthetic booking for development.",
                 cancelled_by="guest" if status == "cancelled" else None,
                 trace_id=f"tr-seed-{n:06d}", publish_pending=False, version=len(path),
                 created_datetime=stamp(created, 8), updated_datetime=stamp(created, 8))
        previous = None
        for j, state in enumerate(path):
            when = stamp(end+timedelta(days=1), 8) if state == "completed" else stamp(created, 8+j)
            actor = guest["guest_id"] if state in {"requested", "cancelled"} else (
                p["host_id"] if state in {"confirmed", "declined"} else (
                    "completion-job" if state == "completed" else "booking-worker"))
            history.append(dict(id=len(history)+1, reservation_id=r["reservation_id"],
                                from_status=previous, to_status=state, actor_id=actor,
                                reason=r["status_reason"] if state == status else None,
                                trace_id=r["trace_id"], changed_datetime=when))
            previous = state
            r["updated_datetime"] = when
        if status in HELD:
            for offset in range(nights):
                blocks.append(dict(property_id=p["property_id"], night_date=(start+timedelta(days=offset)).isoformat(),
                                   reservation_id=r["reservation_id"], created_datetime=stamp(created, 9)))
        reservations.append(r)
        return r

    for i, p in enumerate(props[:197]):
        count = 2 if i == 43 else (8 if i in [0, 6, 11] else 3+i % 6)
        for j in range(count):
            start = anchor-timedelta(days=10+j*5)
            r = reserve(p, "completed", start)
            tag = "positive" if i in [0, 43] else (
                ["cleanliness", "cancellation", "missing_amenity"][j % 3] if i == 6 else (
                    "positive" if j % 3 else list(TEXTS)[1+i % 6]))
            rating = 5 if tag == "positive" else 2
            response = "Thank you for your feedback. This is a synthetic host response." if j % 2 == 0 else None
            reviews.append(dict(review_id=opaque("rev-", len(reviews)+1), reservation_id=r["reservation_id"],
                                property_id=p["property_id"], guest_id=r["guest_id"], host_id=p["host_id"],
                                rating=rating, review_text=TEXTS[tag], tags=[tag],
                                created_datetime=stamp(start+timedelta(days=4)), response_text=response,
                                response_datetime=stamp(start+timedelta(days=5)) if response else None,
                                trace_id=f"tr-review-{len(reviews)+1:06d}"))
        future_status = ["requested", "pending_host", "confirmed", "rejected", "declined", "cancelled"][i % 6]
        if i == 43:
            future_status = "requested"  # low-review candidate stays available for the shared scenario
        # A rejected request overlaps an existing confirmed stay to provide real conflict evidence.
        if future_status == "rejected":
            reserve(p, "confirmed", anchor+timedelta(days=7))
        reserve(p, future_status, anchor+timedelta(days=7))
        # Do not label a partially occupied listing as wholly unavailable.
        if any(b["property_id"] == p["property_id"] and b["night_date"] >= anchor.isoformat() for b in blocks):
            p["availability"] = "partially_available"
    saved = []
    for guest in guests:
        for p in rng.sample(props[:197], 3):
            saved.append(dict(guest_id=guest["guest_id"], property_id=p["property_id"], saved_datetime=stamp(anchor-timedelta(days=3))))
    conversations, messages = [], []
    for r in reservations[:80]:
        cid = opaque("conv-", len(conversations)+1)
        conversations.append(dict(conversation_id=cid, guest_id=r["guest_id"], host_id=r["host_id"],
                                  reservation_id=r["reservation_id"], created_datetime=r["created_datetime"]))
        for j, body in enumerate(["Could you confirm the check-in instructions?", "Yes, I will send the instructions before arrival."]):
            messages.append(dict(message_id=opaque("msg-", len(messages)+1), conversation_id=cid,
                                 reservation_id=r["reservation_id"], sender_id=r["guest_id"] if j == 0 else r["host_id"],
                                 recipient_id=r["host_id"] if j == 0 else r["guest_id"], body=body,
                                 created_datetime=r["created_datetime"].replace("08:00", f"{8+j:02d}:00"),
                                 delivery_status="sent", trace_id=f"tr-message-{len(messages)+1:06d}"))
    by_property = defaultdict(list)
    for review in reviews:
        by_property[review["property_id"]].append(review["rating"])
    for p in props:
        ratings = by_property[p["property_id"]]
        p.update(review_count=len(ratings), rating=round(sum(ratings)/len(ratings), 4) if ratings else 0.0)
    saves = Counter(s["guest_id"] for s in saved)
    bookings = Counter(r["guest_id"] for r in reservations)
    for g in guests:
        g.update(saved_properties_count=saves[g["guest_id"]], booking_count=bookings[g["guest_id"]])
    # Derived from host decision history; pending requests are excluded from the denominator.
    for h in hosts:
        decisions = [r for r in reservations if r["host_id"] == h["host_id"] and r["status"] in {"confirmed", "declined", "cancelled", "completed"}]
        h.update(response_rate=1.0 if decisions else 0.0, response_time=60 if decisions else None)
    data = dict(guests=guests, hosts=hosts, properties=props, saved_properties=saved,
                reservations=reservations, availability_blocks=blocks,
                reservation_status_history=history, reviews=reviews, conversations=conversations, messages=messages)
    return data


def validate(data, anchor):
    """Validate records loaded from disk too; explicit errors remain active under python -O."""
    def require(condition, message):
        if not condition:
            raise ValueError(message)
    def index(name, field):
        rows = data[name]
        result = {r[field]: r for r in rows}
        require(len(result) == len(rows), f"Duplicate {field}")
        return result
    guests, hosts = index("guests", "guest_id"), index("hosts", "host_id")
    props, res = index("properties", "property_id"), index("reservations", "reservation_id")
    index("reviews", "review_id")
    index("messages", "message_id")
    index("reservation_status_history", "id")
    conv = index("conversations", "conversation_id")
    require(len(props) == 200, "Expected fixed 200-property pool")
    for users in [guests, hosts]:
        require(len({u["email"] for u in users.values()}) == len(users), "Duplicate email")
    expected_blocks = set()
    for p in props.values():
        require(p["host_id"] in hosts, "Unknown property host")
        require(p["nightly_price"] > 0 and p["cleaning_fee"] >= 0, "Invalid property price")
        require(p["property_type"] in TYPES and p["status"] in {"active", "draft", "suspended", "archived"}, "Invalid listing enum")
        require(1 <= p["capacity"] <= 30, "Invalid capacity")
    hist = defaultdict(list)
    for h in data["reservation_status_history"]:
        require(h["reservation_id"] in res, "Orphan history")
        hist[h["reservation_id"]].append(h)
    for r in res.values():
        require(r["property_id"] in props and r["guest_id"] in guests, "Orphan reservation")
        p = props[r["property_id"]]
        require(p["host_id"] == r["host_id"] and p["status"] == "active", "Reservation host/status mismatch")
        start, end = date.fromisoformat(r["check_in"]), date.fromisoformat(r["check_out"])
        require(1 <= (end-start).days == r["nights"] <= 30, "Invalid stay dates")
        require(r["status"] in STATUSES and 1 <= r["guest_count"] <= p["capacity"], "Invalid booking status/capacity")
        require(r["total_price"] == price_total(r["nightly_price"], r["nights"], r["cleaning_fee"]), "Incorrect total")
        require((r["status"] == "completed" and end < anchor) or (r["status"] != "completed" and start >= anchor), "Booking inconsistent with anchor")
        rows = sorted(hist[r["reservation_id"]], key=lambda h: h["changed_datetime"])
        path = ["requested"] + [x for x in TRANSITIONS[r["status"]] if x not in {None, "requested"}] + ([] if r["status"] == "requested" else [r["status"]])
        require([h["to_status"] for h in rows] == path, "Invalid status history path")
        require([h["from_status"] for h in rows] == [None]+path[:-1], "Broken history chain")
        require(r["version"] == len(rows) and r["updated_datetime"] == rows[-1]["changed_datetime"], "History/version mismatch")
        require(r["created_datetime"] <= r["updated_datetime"], "Reversed timestamps")
        if r["status"] in HELD:
            for j in range(r["nights"]):
                expected_blocks.add((r["property_id"], (start+timedelta(days=j)).isoformat(), r["reservation_id"]))
    actual_blocks = {(b["property_id"], b["night_date"], b["reservation_id"]) for b in data["availability_blocks"]}
    require(actual_blocks == expected_blocks, "Missing/extra availability blocks")
    require(len({(b[0], b[1]) for b in actual_blocks}) == len(data["availability_blocks"]), "Duplicate held property night")
    ratings, reviewed = defaultdict(list), set()
    for v in data["reviews"]:
        require(v["reservation_id"] in res and v["reservation_id"] not in reviewed, "Orphan/duplicate review")
        reviewed.add(v["reservation_id"])
        r = res[v["reservation_id"]]
        require(r["status"] == "completed", "Review for non-completed stay")
        require(all(v[k] == r[k] for k in ["property_id", "guest_id", "host_id"]), "Review identity mismatch")
        require(r["check_out"] < v["created_datetime"][:10] <= anchor.isoformat(), "Invalid review date")
        require(1 <= v["rating"] <= 5 and 10 <= len(v["review_text"]) <= 2000, "Invalid review fields")
        require(set(v["tags"]) <= set(TEXTS), "Invalid review tags")
        require((v["response_text"] is None) == (v["response_datetime"] is None), "Incomplete response")
        if v["response_datetime"]:
            require(v["created_datetime"] <= v["response_datetime"] <= stamp(anchor, 23), "Invalid response date")
        ratings[v["property_id"]].append(v["rating"])
    for p in props.values():
        values = ratings[p["property_id"]]
        require(p["review_count"] == len(values), "Incorrect review count")
        require(p["rating"] == (round(sum(values)/len(values), 4) if values else 0.0), "Incorrect aggregate rating")
    saved = data["saved_properties"]
    require(len({(s["guest_id"], s["property_id"]) for s in saved}) == len(saved), "Duplicate saved property")
    for s in saved:
        require(s["guest_id"] in guests and s["property_id"] in props, "Orphan save")
    for g in guests.values():
        require(g["saved_properties_count"] == sum(s["guest_id"] == g["guest_id"] for s in saved), "Incorrect save count")
        require(g["booking_count"] == sum(r["guest_id"] == g["guest_id"] for r in res.values()), "Incorrect booking count")
    for c in conv.values():
        require(c["reservation_id"] in res, "Orphan conversation")
        require(all(c[k] == res[c["reservation_id"]][k] for k in ["guest_id", "host_id"]), "Conversation identity mismatch")
    for m in data["messages"]:
        require(m["conversation_id"] in conv, "Orphan message")
        c = conv[m["conversation_id"]]
        require({m["sender_id"], m["recipient_id"]} == {c["guest_id"], c["host_id"]}, "Invalid participants")
        require(m["reservation_id"] == c["reservation_id"], "Message booking mismatch")
        require(m["delivery_status"] in {"queued", "sent", "failed"} and 1 <= len(m["body"]) <= 4000, "Invalid message")
    require({r["status"] for r in res.values()} == STATUSES, "Missing status coverage")
    return {name: len(rows) for name, rows in data.items()}


def encoded(value):
    return (json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False)+"\n").encode("utf-8")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=260)
    parser.add_argument("--anchor-date", type=date.fromisoformat, default=date(2026, 11, 24))
    parser.add_argument("--output", type=Path, default=Path("data/seed/generated"))
    parser.add_argument("--validate-only", action="store_true", help="Validate existing files and checksums")
    parser.add_argument("--overwrite", action="store_true", help="Replace only files named by this generator")
    args = parser.parse_args()
    if args.validate_only:
        manifest = json.loads((args.output/"manifest.json").read_text())
        data = {name: json.loads((args.output/(name+".json")).read_text()) for name in manifest["counts"]}
        for filename, digest in manifest["sha256"].items():
            if hashlib.sha256((args.output/filename).read_bytes()).hexdigest() != digest:
                raise ValueError("Checksum mismatch: "+filename)
        counts = validate(data, date.fromisoformat(manifest["anchor_date"]))
        if counts != manifest["counts"]:
            raise ValueError("Manifest counts differ")
    else:
        data = build(args.seed, args.anchor_date)
        counts = validate(data, args.anchor_date)
        blobs = {name+".json": encoded(rows) for name, rows in data.items()}
        manifest = dict(generator_version="1.0", seed=args.seed, anchor_date=args.anchor_date.isoformat(),
                        pool_id="benchmark-200-properties-v1", dataset_version="seed-v1-"+args.anchor_date.isoformat(),
                        counts=counts, sha256={name: hashlib.sha256(blob).hexdigest() for name, blob in blobs.items()},
                        scenarios={"strong_candidate": "property-2001", "risk_complaints": "property-2007",
                                   "few_reviews_candidate": "property-2044", "occupied_dates_property": "property-2002",
                                   "check_in": (args.anchor_date+timedelta(days=7)).isoformat(),
                                   "check_out": (args.anchor_date+timedelta(days=9)).isoformat()},
                        notes=["All data are synthetic; no real listings or accounts.",
                               "Conversations are a proposed helper fixture, not a finalized storage schema.",
                               "No credentials, Kafka history, AI benchmark labels or evaluated AI results are seeded.",
                               "booking_count counts all reservation rows; confirm dashboard semantics with A/B.",
                               "Historical completed-night blocks are retained as documented in booking.md.",
                               "Use this dataset_version consistently; API example version 2026-10-30 is illustrative."])
        blobs["manifest.json"] = encoded(manifest)
        if not args.overwrite and any((args.output/name).exists() for name in blobs):
            parser.error("Output already exists. Use another --output or explicitly --overwrite.")
        args.output.mkdir(parents=True, exist_ok=True)
        for name, blob in blobs.items():
            (args.output/name).write_bytes(blob)
    print(json.dumps({"status": "PASS", "counts": counts}, indent=2))


if __name__ == "__main__":
    main()
