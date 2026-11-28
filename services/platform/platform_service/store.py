"""Durable event log: unique event and operation IDs prevent double counting on replay."""
import json
from pathlib import Path

from pymongo import ASCENDING
from pymongo.errors import DuplicateKeyError

from shared.consumer import validate
from shared.consumer import now

ROOT = Path(__file__).resolve().parents[3]


def initialize(db):
    for collection, field in [('messages', 'message_id'), ('conversations', 'conversation_id'),
                              ('reviews', 'review_id'), ('debates', 'trace_id')]:
        db[collection].create_index(field, unique=True)
    db.messages.create_index([('sender_id', 1), ('http_key', 1)], unique=True,
                             partialFilterExpression={'http_key': {'$type': 'string'}})
    db.messages.create_index([('conversation_id', 1), ('created_datetime', 1), ('message_id', 1)])
    db.messages.create_index('publish_pending')
    db.conversations.create_index('reservation_id', unique=True)
    db.reviews.create_index('reservation_id', unique=True)
    db.reviews.create_index([('property_id', 1), ('created_datetime', -1)])
    db.event_traces.create_index('event_id', unique=True)
    db.event_traces.create_index('idempotency_key', unique=True)
    db.event_traces.create_index([('payload.property_id', 1), ('timestamp', 1)])
    db.event_traces.create_index('trace_id')


def ingest(db, event):
    validate(event)
    accepted = True
    try:
        db.event_traces.insert_one({**event, '_platform_received_at': now()})
    except DuplicateKeyError:
        accepted = False
    # Idempotent projection is rerun even after a partially completed attempt.
    if event['event_type'] == 'booking.updated':
        p = event['payload']
        if all(p.get(k) for k in ('reservation_id', 'guest_id', 'host_id')):
            db.conversations.update_one({'reservation_id': p['reservation_id']},
                                        {'$setOnInsert': {'conversation_id': 'conv-' + p['reservation_id'],
                                                         'reservation_id': p['reservation_id'],
                                                         'guest_id': p['guest_id'], 'host_id': p['host_id'],
                                                         'created_datetime': event['timestamp']}}, upsert=True)
    return accepted


def seed(db, directory=None):
    directory = Path(directory or ROOT / 'data/seed/generated')
    for collection, field in [('conversations', 'conversation_id'), ('messages', 'message_id'), ('reviews', 'review_id')]:
        for row in json.loads((directory / f'{collection}.json').read_text()):
            db[collection].update_one({field: row[field]}, {'$setOnInsert': row}, upsert=True)


def properties():
    return {p['property_id']: p for p in json.loads((ROOT / 'data/seed/generated/properties.json').read_text())}
