"""Coarse namespace invalidation; repeating it never changes domain data."""
from datetime import date
import hashlib
import uuid

from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError, BusyLoadingError
from shared.consumer import PermanentError, RecordMeta, process_record, validate
from .search_cache import make_client, prefix
from .rating_worker import AcknowledgedPublisher
from .review_ratings import review_payload

GROUP = "property-cache"
TOPICS = ("property.created", "property.updated", "property.archived", "property.availability.updated", "review.created")
TRANSIENT = (RedisConnectionError, RedisTimeoutError, BusyLoadingError)

INVALIDATE = """
if redis.call('EXISTS', KEYS[2]) == 1 then return 0 end
redis.call('SET', KEYS[1], ARGV[1])
redis.call('SET', KEYS[2], '1', 'EX', 604800)
redis.call('INCR', KEYS[3])
return 1
"""


def check_payload(event):
    validate(event)
    kind, p = event["event_type"], event["payload"]
    if kind not in TOPICS:
        raise PermanentError("UNEXPECTED_CACHE_EVENT", "Unsupported invalidation event type")
    pid = p.get("property_id")
    if not isinstance(pid, str) or not pid.strip() or len(pid) > 64 or not pid.isascii():
        raise PermanentError("INVALID_CACHE_EVENT", "A bounded ASCII property ID is required")
    if kind != "review.created" and event["entity"] != {"entity_type": "property", "entity_id": pid}:
        raise PermanentError("INVALID_CACHE_EVENT", "Entity must identify the property")
    if kind == "review.created":
        review_payload(event)
    elif kind == "property.availability.updated":
        try:
            if p["change"] not in {"held", "released"} or not isinstance(p["reservation_id"], str) or not p["reservation_id"].strip() or len(p["reservation_id"]) > 64 or not p["reservation_id"].isascii():
                raise ValueError()
            nights = p["nights"]
            if not isinstance(nights, list) or not 1 <= len(nights) <= 30:
                raise ValueError()
            if len(set(nights)) != len(nights) or any(date.fromisoformat(n).isoformat() != n for n in nights):
                raise ValueError()
        except (ValueError, TypeError, KeyError):
            raise PermanentError("INVALID_CACHE_EVENT", "Invalid availability change payload") from None
    elif type(p.get("version")) is not int or p["version"] < 1 or not isinstance(p.get("host_id"), str) or not p["host_id"].strip():
        raise PermanentError("INVALID_CACHE_EVENT", "Listing version and host ID are required")


class CacheInvalidator:
    def __init__(self, settings, *, client=None):
        self.redis = client if client is not None else make_client(settings)
        self.prefix = prefix(settings)

    def apply(self, event):
        check_payload(event)
        receipt = self.prefix + "receipt:" + hashlib.sha256(event["idempotency_key"].encode()).hexdigest()
        return self.redis.eval(INVALIDATE, 3, self.prefix + "generation", receipt,
                               self.prefix + "invalidations", uuid.uuid4().hex)


def handle(record, invalidator, publish, commit, *, sleep=None):
    meta = RecordMeta(record.topic(), record.partition(), record.offset(),
                      record.key().decode(errors="replace") if record.key() is not None else None)
    return process_record(record.value(), meta, invalidator.apply, publish, commit,
                          group=GROUP, is_transient=lambda e: isinstance(e, TRANSIENT),
                          **({"sleep": sleep} if sleep is not None else {}))


def run(settings):
    from confluent_kafka import Consumer
    import logging
    log = logging.getLogger(__name__)
    invalidator, publish = CacheInvalidator(settings), AcknowledgedPublisher(settings.kafka_bootstrap)
    consumer = Consumer({"bootstrap.servers": settings.kafka_bootstrap, "group.id": GROUP,
                         "enable.auto.commit": False, "enable.auto.offset.store": False,
                         "auto.offset.reset": "earliest", "allow.auto.create.topics": False})
    consumer.subscribe(list(TOPICS))
    log.info("property-cache ready; subscribed to invalidation topics")
    try:
        while True:
            record = consumer.poll(1)
            if record is None: continue
            if record.error(): raise RuntimeError(str(record.error()))
            result = handle(record, invalidator, publish, lambda: consumer.commit(message=record, asynchronous=False))
            log.info("%s topic=%s partition=%d offset=%d", result, record.topic(), record.partition(), record.offset())
    finally:
        consumer.close()
