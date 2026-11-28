"""Envelope validation and bounded delivery. Commit only after durable processing or DLQ ack.

Business handlers must persist their idempotency receipt with their side effect atomically.
The platform uses immutable, uniquely indexed Mongo event documents as both receipt and projection.
"""
import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

BACKOFF_S = (0.2, 1.0, 5.0)
SCHEMA = json.loads(Path(__file__).with_name('envelope.schema.json').read_text())
VALIDATOR = Draft202012Validator(SCHEMA, format_checker=FormatChecker())
TOPICS = (
    'booking.requested', 'booking.updated', 'property.created', 'property.updated',
    'property.archived', 'property.viewed', 'property.saved', 'property.availability.updated',
    'review.created', 'review.responded', 'message.created', 'ai.requests', 'ai.parsed',
    'debate.round.0', 'debate.round.1', 'debate.round.2', 'ai.results', 'ai.approvals',
)


def now():
    return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


class PermanentError(Exception):
    def __init__(self, code, message):
        self.code, self.message = code, message
        super().__init__(message)


class RetryExhausted(Exception):
    def __init__(self, last, retries):
        self.last, self.retries = last, retries
        super().__init__(str(last))


@dataclass(frozen=True)
class RecordMeta:
    topic: str
    partition: int
    offset: int
    key: str | None


def build(event_type, actor_id, entity_type, entity_id, payload, idempotency_key, trace_id=None):
    return dict(event_type=event_type, actor_id=actor_id,
                entity={'entity_type': entity_type, 'entity_id': entity_id}, payload=payload,
                idempotency_key=idempotency_key, trace_id=trace_id or 'tr-' + uuid.uuid4().hex,
                event_id='ev-' + uuid.uuid4().hex, timestamp=now(), attempt=1,
                producer='platform-service', schema_version='1.0')


def validate(event, *, key=None, check_key=False):
    problem = next(VALIDATOR.iter_errors(event), None)
    if problem:
        raise PermanentError('INVALID_EVENT_ENVELOPE', problem.message)
    try:
        stamp = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00'))
        if stamp.tzinfo is None:
            raise ValueError('timezone required')
    except ValueError:
        raise PermanentError('INVALID_EVENT_ENVELOPE', 'timestamp must be ISO8601 with timezone') from None
    etype, payload = event['event_type'], event['payload']
    if etype not in TOPICS and etype != 'platform.dlq':
        raise PermanentError('UNKNOWN_EVENT_TYPE', etype)
    field = ('conversation_id' if etype.startswith('message.') else
             'property_id' if etype.startswith(('property.', 'booking.', 'review.')) else None)
    if field and not isinstance(payload.get(field), str):
        raise PermanentError('INVALID_PAYLOAD', f'{field} must be a string')
    if etype == 'property.saved' and type(payload.get('saved')) is not bool:
        raise PermanentError('INVALID_PAYLOAD', 'saved must be boolean')
    if etype == 'booking.updated':
        if payload.get('to_status') not in ('requested', 'pending_host', 'confirmed', 'rejected', 'declined', 'cancelled', 'completed'):
            raise PermanentError('INVALID_PAYLOAD', 'invalid to_status')
        if not isinstance(payload.get('reservation_id'), str) or type(payload.get('version')) is not int or payload['version'] < 1:
            raise PermanentError('INVALID_PAYLOAD', 'reservation_id and positive version required')
    expected = payload[field] if field else event['trace_id']
    if check_key and etype != 'platform.dlq' and key != expected:
        raise PermanentError('EVENT_KEY_MISMATCH', f'expected key {expected}')
    return event


def run_with_retry(fn, *, is_transient, backoff=BACKOFF_S, sleep=time.sleep, label=''):
    for retry in range(len(backoff) + 1):
        try:
            return fn(), retry
        except Exception as exc:
            if not is_transient(exc):
                raise
            if retry == len(backoff):
                raise RetryExhausted(exc, retry) from exc
            logging.getLogger('platform.consumer').warning('transient error %s retry=%d/%d in %.1fs: %r',
                                                          label, retry + 1, len(backoff), backoff[retry], exc)
            sleep(backoff[retry])


def process_record(value, meta, handler, publish, commit, *, group, is_transient, sleep=time.sleep):
    """publish(topic, key, event) must wait for broker acknowledgement or raise."""
    original = value.decode(errors='replace') if isinstance(value, bytes) else value
    retries = 0
    try:
        try:
            original = json.loads(value)
        except (ValueError, TypeError, UnicodeError):
            raise PermanentError('INVALID_JSON', 'record is not valid JSON') from None
        validate(original, key=meta.key, check_key=True)
        if original['event_type'] != meta.topic:
            raise PermanentError('UNKNOWN_EVENT_TYPE', 'topic/event_type mismatch')
        _, retries = run_with_retry(lambda: handler(original), is_transient=is_transient, sleep=sleep)
    except (PermanentError, RetryExhausted) as exc:
        if meta.topic == 'platform.dlq':
            # Quarantine-topic failures need an operator; never create a recursive DLQ loop.
            raise
        retries = getattr(exc, 'retries', 0)
        source = original if isinstance(original, dict) else {}
        payload = dict(original_topic=meta.topic, original_partition=meta.partition,
                       original_offset=meta.offset, original_key=meta.key, original_event=original,
                       original_event_id=source.get('event_id'), trace_id=source.get('trace_id'),
                       error_code=getattr(exc, 'code', 'RETRY_EXHAUSTED'), error_message=str(exc)[:1000],
                       retry_count=retries, failed_at=now(), consumer_group=group)
        dead = build('platform.dlq', group, 'event', source.get('event_id') or str(meta.offset),
                     payload, f'dlq:{group}:{meta.topic}:{meta.partition}:{meta.offset}', source.get('trace_id'))
        publish('platform.dlq', meta.key, dead)
        commit()
        return 'dlq'
    commit()
    return 'processed'
