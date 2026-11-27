"""Review storage (docs/api/review.md, MongoDB collection `reviews`).

Two implementations with the same methods:
  MongoReviewStore   - production, pymongo
  MemoryReviewStore  - tests and offline runs, no database needed
Uniqueness that protects correctness lives in the store (unique index in
Mongo, a dict check in memory): one review per reservation, one review per
idempotency key, one host response per review.
"""
from collections import Counter

SORTS = {"newest": ("created_datetime", -1), "highest": ("rating", -1), "lowest": ("rating", 1)}


class DuplicateReview(Exception):
    """A review already exists for this reservation (or idempotency key)."""


class AlreadyResponded(Exception):
    """The host has already responded to this review."""


def _public(doc):
    return None if doc is None else {k: v for k, v in doc.items() if k != "_id"}


def _summary(docs):
    docs = list(docs)
    tags = Counter(t for d in docs for t in d.get("tags", []))
    return {"review_count": len(docs),
            "average_rating": round(sum(d["rating"] for d in docs) / len(docs), 2) if docs else None,
            "tag_counts": dict(sorted(tags.items()))}


class MongoReviewStore:
    def __init__(self, db):
        self.col = db["reviews"]

    def ensure_indexes(self):
        self.col.create_index("review_id", unique=True)
        self.col.create_index("reservation_id", unique=True)
        self.col.create_index("idempotency_key", unique=True, sparse=True)
        self.col.create_index([("property_id", 1), ("created_datetime", -1)])
        self.col.create_index([("host_id", 1), ("created_datetime", -1)])
        self.col.create_index([("property_id", 1), ("tags", 1)])        # Risk agent: complaints per property
        self.col.create_index("publish_pending", sparse=True)

    def insert(self, doc):
        from pymongo.errors import DuplicateKeyError
        try:
            self.col.insert_one(dict(doc))
        except DuplicateKeyError as e:
            raise DuplicateReview(str(e)) from None

    def get(self, review_id):
        return _public(self.col.find_one({"review_id": review_id}))

    def by_idempotency_key(self, key):
        return _public(self.col.find_one({"idempotency_key": key}))

    def by_property(self, property_id, sort="newest", min_rating=None, tag=None, page=1, page_size=20):
        q = {"property_id": property_id}
        if min_rating is not None:
            q["rating"] = {"$gte": min_rating}
        if tag:
            q["tags"] = tag
        field, direction = SORTS[sort]
        cur = (self.col.find(q, {"_id": 0}).sort([(field, direction), ("review_id", 1)])
               .skip((page - 1) * page_size).limit(page_size))
        return list(cur), self.col.count_documents(q)

    def summary(self, property_id):
        return _summary(self.col.find({"property_id": property_id}, {"rating": 1, "tags": 1}))

    def set_response(self, review_id, text, when):
        # atomic: only matches while no response exists, so a second reply cannot overwrite the first
        res = self.col.find_one_and_update(
            {"review_id": review_id, "response_text": None},
            {"$set": {"response_text": text, "response_datetime": when}},
            projection={"_id": 0}, return_document=True)
        if res is None:
            if self.col.count_documents({"review_id": review_id}) == 0:
                return None
            raise AlreadyResponded(review_id)
        return res

    def mark_published(self, review_id, event_type):
        self.col.update_one({"review_id": review_id}, {"$pull": {"publish_pending": event_type}})

    def pending(self):
        return list(self.col.find({"publish_pending": {"$exists": True, "$ne": []}}, {"_id": 0}))


class MemoryReviewStore:
    def __init__(self):
        self.docs = {}

    def ensure_indexes(self):
        pass

    def insert(self, doc):
        for d in self.docs.values():
            if d["reservation_id"] == doc["reservation_id"]:
                raise DuplicateReview(doc["reservation_id"])
            if doc.get("idempotency_key") and d.get("idempotency_key") == doc["idempotency_key"]:
                raise DuplicateReview(doc["idempotency_key"])
        self.docs[doc["review_id"]] = dict(doc)

    def get(self, review_id):
        d = self.docs.get(review_id)
        return dict(d) if d else None

    def by_idempotency_key(self, key):
        return next((dict(d) for d in self.docs.values() if d.get("idempotency_key") == key), None)

    def by_property(self, property_id, sort="newest", min_rating=None, tag=None, page=1, page_size=20):
        docs = [d for d in self.docs.values() if d["property_id"] == property_id
                and (min_rating is None or d["rating"] >= min_rating)
                and (not tag or tag in d.get("tags", []))]
        field, direction = SORTS[sort]
        docs.sort(key=lambda d: d["review_id"])
        docs.sort(key=lambda d: d[field], reverse=direction < 0)
        start = (page - 1) * page_size
        return [dict(d) for d in docs[start:start + page_size]], len(docs)

    def summary(self, property_id):
        return _summary(d for d in self.docs.values() if d["property_id"] == property_id)

    def set_response(self, review_id, text, when):
        d = self.docs.get(review_id)
        if d is None:
            return None
        if d.get("response_text") is not None:
            raise AlreadyResponded(review_id)
        d.update(response_text=text, response_datetime=when)
        return dict(d)

    def mark_published(self, review_id, event_type):
        d = self.docs.get(review_id)
        if d:
            d["publish_pending"] = [e for e in d.get("publish_pending", []) if e != event_type]

    def pending(self):
        return [dict(d) for d in self.docs.values() if d.get("publish_pending")]
