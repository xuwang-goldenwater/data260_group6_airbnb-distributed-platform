import json
import time
import jwt
import mongomock
import pytest
from fastapi.testclient import TestClient
from platform_service.app import create_app
from platform_service.store import initialize, ingest, seed
from platform_service.worker import flush_outbox
from shared.consumer import PermanentError, RecordMeta, build, process_record, validate

SECRET = 'test-only-secret-of-at-least-thirty-two-characters'

def auth(sub='guest-1001', role='guest'):
    return {'Authorization': 'Bearer ' + jwt.encode({'sub': sub, 'role': role, 'exp': int(time.time()) + 300}, SECRET, algorithm='HS256')}

@pytest.fixture
def ctx():
    db = mongomock.MongoClient().airbnb
    initialize(db)
    seed(db)
    return db, TestClient(create_app(db, SECRET))

def event(kind='property.viewed', **payload):
    return build(kind, 'guest-1001', 'property', 'property-2001', {'property_id': 'property-2001', **payload}, 'op-' + str(time.time_ns()))

@pytest.mark.parametrize('field', ['event_id', 'event_type', 'trace_id', 'timestamp', 'actor_id', 'entity', 'payload', 'idempotency_key', 'attempt', 'producer', 'schema_version'])
def test_required_envelope_fields(field):
    row = event()
    del row[field]
    with pytest.raises(PermanentError): validate(row)

@pytest.mark.parametrize('field,value', [('timestamp', 'not-a-date'), ('schema_version', '2.0'), ('payload', []), ('attempt', 0)])
def test_bad_envelope(field, value):
    row = event()
    row[field] = value
    with pytest.raises(PermanentError): validate(row)

def test_seed_idempotent(ctx):
    db, _ = ctx
    count = db.messages.count_documents({})
    seed(db)
    assert db.messages.count_documents({}) == count

def test_ingest_auth_validation_and_duplicate(ctx):
    db, client = ctx
    row = event()
    assert client.post('/events/ingest', json=row).status_code == 401
    assert client.post('/events/ingest', json=row, headers=auth()).status_code == 403
    headers = auth('booking-service', 'service')
    assert client.post('/events/ingest', json=row, headers=headers).status_code == 202
    assert client.post('/events/ingest', json=row, headers=headers).status_code == 200
    row['event_id'] = 'another-event'
    assert client.post('/events/ingest', json=row, headers=headers).status_code == 200
    assert db.event_traces.count_documents({}) == 1
    assert client.post('/events/ingest', json=event(), headers={**headers, 'X-Event-Key': 'wrong'}).status_code == 422

def test_host_funnel_matches_booking_client(ctx):
    db, client = ctx
    ingest(db, event())
    ingest(db, event('property.saved', saved=True))
    ingest(db, event('property.saved', saved=False))
    result = client.get('/analytics/host/host-501?days=30&property_id=property-2001', headers=auth('booking-service', 'service'))
    assert result.status_code == 200
    assert result.json()['funnel'] == {'views': 1, 'saves': 1, 'unsaves': 1}
    assert client.get('/analytics/host/host-501', headers=auth()).status_code == 403
    assert client.get('/analytics/host/host-501', headers=auth('host-501', 'host')).status_code == 200

def test_message_outbox_replay_authorization(ctx):
    db, client = ctx
    conv = db.conversations.find_one({'guest_id': 'guest-1001'})
    body = dict(conversation_id=conv['conversation_id'], recipient_id=conv['host_id'], body='Hello host')
    headers = {**auth(), 'Idempotency-Key': 'test-send-key'}
    first = client.post('/messages/send', json=body, headers=headers)
    assert first.status_code == 201, first.text
    assert client.post('/messages/send', json=body, headers=headers).json()['message_id'] == first.json()['message_id']
    assert client.post('/messages/send', json={**body, 'body': 'Changed'}, headers=headers).status_code == 422
    assert client.post('/messages/send', json=body, headers={**auth('guest-1002'), 'Idempotency-Key': 'other'}).status_code == 403
    assert client.post('/messages/send', json={**body, 'sender_id': 'host-501'}, headers=headers).status_code == 400
    published = []
    flush_outbox(db, lambda *args: published.append(args))
    flush_outbox(db, lambda *args: published.append(args))
    assert len(published) == 1
    assert published[0][0] == 'message.created'
    assert db.messages.find_one({'message_id': first.json()['message_id']})['delivery_status'] == 'sent'

def test_outbox_failure_remains_pending(ctx):
    db, _ = ctx
    db.messages.insert_one({'message_id': 'pending', 'conversation_id': 'c', 'event': {}, 'publish_pending': True})
    def fail(*args): raise RuntimeError('broker offline')
    with pytest.raises(RuntimeError): flush_outbox(db, fail)
    assert db.messages.find_one({'message_id': 'pending'})['publish_pending']

def test_dlq_ack_before_commit_and_retry():
    actions, delays = [], []
    def fail(event): raise ConnectionError('offline')
    result = process_record(json.dumps(event()), RecordMeta('property.viewed', 0, 1, 'property-2001'), fail,
                            lambda *args: actions.append(('dlq', args[2])), lambda: actions.append(('commit',)),
                            group='analytics', is_transient=lambda e: isinstance(e, ConnectionError), sleep=delays.append)
    assert result == 'dlq'
    assert delays == [0.2, 1, 5]
    assert [a[0] for a in actions] == ['dlq', 'commit']
    assert actions[0][1]['payload']['retry_count'] == 3

def test_failed_dlq_never_commits():
    commits = []
    def fail(*args): raise RuntimeError('no acknowledgement')
    with pytest.raises(RuntimeError):
        process_record(b'not-json', RecordMeta('property.viewed', 0, 0, None), lambda e: None, fail,
                       lambda: commits.append(1), group='analytics', is_transient=lambda e: False)
    assert not commits

def test_booking_out_of_order_uses_highest_version(ctx):
    db, client = ctx
    ingest(db, event('booking.updated', reservation_id='r1', version=3, to_status='confirmed'))
    ingest(db, event('booking.updated', reservation_id='r1', version=2, to_status='pending_host'))
    assert client.get('/analytics/platform', headers=auth('admin', 'admin')).json()['reservations'] == {'confirmed': 1}

def test_malformed_json_goes_to_dlq():
    records, commits = [], []
    process_record(b'bad-json', RecordMeta('property.viewed', 1, 4, 'p'), lambda e: None,
                   lambda t,k,e: records.append(e), lambda: commits.append(1),
                   group='analytics', is_transient=lambda e: False)
    assert records[0]['payload']['error_code'] == 'INVALID_JSON'
    assert commits == [1]

def test_booking_creates_conversation_on_replay(ctx):
    db, client = ctx
    row = event('booking.updated', reservation_id='new-reservation', guest_id='guest-1001',
                host_id='host-501', version=2, to_status='pending_host')
    assert ingest(db, row)
    assert not ingest(db, row)
    assert db.conversations.count_documents({'reservation_id': 'new-reservation'}) == 1
    result = client.post('/messages/conversation', json={'conversation_id': 'conv-new-reservation'}, headers=auth())
    assert result.status_code == 200 and result.json()['items'] == []

def test_dlq_failure_cannot_recursively_dead_letter():
    published, commits = [], []
    with pytest.raises(PermanentError):
        process_record(b'bad-json', RecordMeta('platform.dlq', 0, 1, 'x'), lambda e: None,
                       lambda *a: published.append(a), lambda: commits.append(1),
                       group='analytics', is_transient=lambda e: False)
    assert not published and not commits

def test_unknown_type_and_key_rejected():
    with pytest.raises(PermanentError) as exc: validate(event('unknown.event'))
    assert exc.value.code == 'UNKNOWN_EVENT_TYPE'
    with pytest.raises(PermanentError) as exc: validate(event(), key='wrong', check_key=True)
    assert exc.value.code == 'EVENT_KEY_MISMATCH'

def test_persist_then_crash_replay_no_double_count(ctx):
    db, _ = ctx
    row = event()
    def crash_commit(): raise RuntimeError('simulated crash after persistence')
    meta = RecordMeta('property.viewed', 0, 1, 'property-2001')
    with pytest.raises(RuntimeError):
        process_record(json.dumps(row), meta, lambda e: ingest(db, e), lambda *a: None,
                       crash_commit, group='analytics', is_transient=lambda e: False)
    process_record(json.dumps(row), meta, lambda e: ingest(db, e), lambda *a: None,
                   lambda: None, group='analytics', is_transient=lambda e: False)
    assert db.event_traces.count_documents({'event_id': row['event_id']}) == 1

def test_required_thread_routes(ctx):
    db, client = ctx
    conv = db.conversations.find_one({'guest_id': 'guest-1001'})
    opened = client.post('/threads/open', json={'reservation_id': conv['reservation_id']}, headers=auth())
    assert opened.status_code == 200
    tid = opened.json()['thread_id']
    assert client.post('/threads/get', json={'thread_id': tid}, headers=auth()).status_code == 200
    assert client.post('/messages/list', json={'thread_id': tid}, headers=auth()).status_code == 200
    assert client.post('/threads/byUser', json={}, headers=auth()).json()['total'] > 0
    assert client.post('/threads/open', json={'reservation_id': conv['reservation_id']}, headers=auth('guest-1002')).status_code == 403

def test_required_analytics_routes_and_trace(ctx):
    db, client = ctx
    e = event()
    ingest(db, e)
    service = auth('booking-service', 'service')
    for path in ('/analytics/properties/top', '/analytics/funnel', '/analytics/geo', '/analytics/host/dashboard?host_id=host-501'):
        r = client.get(path, headers=service)
        assert r.status_code == 200, (path, r.text)
    assert client.get('/analytics/guest/dashboard', headers=auth()).status_code == 200
    assert client.get('/analytics/traces/' + e['trace_id'], headers=service).json()['items'][0]['event_id'] == e['event_id']
