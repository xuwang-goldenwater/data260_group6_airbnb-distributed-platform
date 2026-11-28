import hashlib
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import jwt
import httpx
from fastapi import Depends, FastAPI, Header, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError

from shared.consumer import PermanentError, build, now, validate
from .store import ROOT, ingest, initialize, properties


class ApiError(Exception):
    def __init__(self, status, code, message):
        self.status, self.code, self.message = status, code, message


class Page(BaseModel):
    model_config = ConfigDict(extra='forbid')
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=50)


class ConversationQuery(Page):
    conversation_id: str


class ReservationQuery(Page):
    reservation_id: str


class ThreadQuery(Page):
    thread_id: str


class ThreadOpen(BaseModel):
    model_config = ConfigDict(extra='forbid')
    reservation_id: str = Field(min_length=1, max_length=128)


class Send(BaseModel):
    model_config = ConfigDict(extra='forbid')
    conversation_id: str = Field(min_length=1, max_length=128)
    recipient_id: str = Field(min_length=1, max_length=128)
    reservation_id: str | None = None
    body: str = Field(min_length=1, max_length=4000)


def create_app(db=None, secret=None):
    app = FastAPI(title='Role C Messaging and Analytics')
    secret = secret if secret is not None else os.getenv('JWT_SECRET', '')
    issuer = os.getenv('JWT_ISSUER') or None
    if db is None:
        db = MongoClient(os.getenv('MONGO_URI', 'mongodb://localhost:27017'), serverSelectionTimeoutMS=3000)[os.getenv('MONGO_DATABASE', 'airbnb')]
    app.state.db = db

    @app.middleware('http')
    async def trace(request, call_next):
        request.state.trace_id = 'tr-' + uuid.uuid4().hex
        response = await call_next(request)
        response.headers['X-Trace-Id'] = request.state.trace_id
        return response

    @app.exception_handler(ApiError)
    async def api_error(request, exc):
        return JSONResponse(status_code=exc.status, content={'error': dict(code=exc.code, message=exc.message,
                            trace_id=request.state.trace_id, retryable=exc.status == 503)})

    @app.exception_handler(RequestValidationError)
    async def invalid(request, exc):
        return await api_error(request, ApiError(400, 'VALIDATION_ERROR', 'Invalid request fields.'))

    @app.exception_handler(PyMongoError)
    async def unavailable(request, exc):
        return await api_error(request, ApiError(503, 'DEPENDENCY_UNAVAILABLE', 'MongoDB unavailable.'))

    def actor(authorization: str | None = Header(default=None)):
        if not secret:
            raise ApiError(503, 'DEPENDENCY_UNAVAILABLE', 'JWT_SECRET is not configured.')
        if not authorization or not authorization.startswith('Bearer '):
            raise ApiError(401, 'UNAUTHORIZED', 'Bearer token required.')
        try:
            claims = jwt.decode(authorization[7:], secret, algorithms=['HS256'], issuer=issuer,
                                options={'require': ['sub', 'exp', 'role']})
        except jwt.PyJWTError:
            raise ApiError(401, 'UNAUTHORIZED', 'Invalid or expired token.') from None
        if claims['role'] not in ('guest', 'host', 'service', 'admin'):
            raise ApiError(403, 'FORBIDDEN', 'Unknown role.')
        return claims

    def internal(user=Depends(actor)):
        if user['role'] not in ('service', 'admin'):
            raise ApiError(403, 'FORBIDDEN', 'Service or admin token required.')
        return user

    def authorized_conversation(cid, user):
        row = db.conversations.find_one({'conversation_id': cid}, {'_id': 0})
        if not row:
            raise ApiError(404, 'CONVERSATION_NOT_FOUND', 'Unknown conversation.')
        if user['role'] == 'service' or user['sub'] not in (row['guest_id'], row['host_id']):
            raise ApiError(403, 'FORBIDDEN', 'Conversation participants only.')
        return row

    def public_message(row):
        return {k: v for k, v in row.items() if k not in ('_id', 'http_key', 'request_hash', 'event', 'publish_pending')}

    def messages(query, page):
        cursor = db.messages.find(query).sort([('created_datetime', 1), ('message_id', 1)])
        return dict(items=[public_message(x) for x in cursor.skip((page.page - 1) * page.page_size).limit(page.page_size)],
                    page=page.page, page_size=page.page_size, total=db.messages.count_documents(query))

    @app.get('/health')
    def health():
        db.command('ping')
        return {'status': 'ok'}

    @app.get('/platform', include_in_schema=False)
    def dashboard():
        return FileResponse(Path(__file__).with_name('panel.html'))

    @app.post('/messages/send')
    def send(body: Send, request: Request, user=Depends(actor), idempotency_key: str = Header(alias='Idempotency-Key')):
        if not idempotency_key or len(idempotency_key) > 128 or not body.body.strip():
            raise ApiError(400, 'VALIDATION_ERROR', 'Non-empty message and idempotency key required.')
        conv = authorized_conversation(body.conversation_id, user)
        recipient = conv['host_id'] if user['sub'] == conv['guest_id'] else conv['guest_id']
        if body.recipient_id != recipient or body.reservation_id not in (None, conv.get('reservation_id')):
            raise ApiError(403, 'FORBIDDEN', 'Recipient or reservation does not match conversation.')
        fingerprint = hashlib.sha256(json.dumps(body.model_dump(), sort_keys=True).encode()).hexdigest()
        row = dict(message_id='msg-' + uuid.uuid4().hex, **body.model_dump(exclude={'reservation_id'}),
                   reservation_id=conv.get('reservation_id'), sender_id=user['sub'],
                   delivery_status='queued', created_datetime=now(), trace_id=request.state.trace_id,
                   http_key=idempotency_key, request_hash=fingerprint, publish_pending=True)
        payload = {k: row[k] for k in ('message_id', 'conversation_id', 'reservation_id', 'sender_id', 'recipient_id', 'created_datetime')}
        row['event'] = build('message.created', user['sub'], 'message', row['message_id'], payload,
                             'message.created:' + row['message_id'], row['trace_id'])
        try:
            # One document atomically persists message, idempotency receipt and outgoing event.
            db.messages.insert_one(row)
        except DuplicateKeyError:
            row = db.messages.find_one({'sender_id': user['sub'], 'http_key': idempotency_key})
            if row is None or row['request_hash'] != fingerprint:
                raise ApiError(422, 'IDEMPOTENCY_KEY_REUSED', 'Key already used with different body.') from None
            return JSONResponse(public_message(row), status_code=200)
        return JSONResponse(public_message(row), status_code=201)

    @app.post('/messages/conversation')
    def conversation(body: ConversationQuery, user=Depends(actor)):
        authorized_conversation(body.conversation_id, user)
        return messages({'conversation_id': body.conversation_id}, body)

    @app.post('/messages/byReservation')
    def by_reservation(body: ReservationQuery, user=Depends(actor)):
        conv = db.conversations.find_one({'reservation_id': body.reservation_id})
        if not conv:
            raise ApiError(404, 'RESERVATION_NOT_FOUND', 'No conversation for reservation.')
        authorized_conversation(conv['conversation_id'], user)
        return messages({'reservation_id': body.reservation_id}, body)

    @app.get('/messages/conversations')
    def conversation_list(user=Depends(actor), page: int = Query(1, ge=1), page_size: int = Query(20, ge=1, le=50)):
        query = {'$or': [{'guest_id': user['sub']}, {'host_id': user['sub']}]}
        return dict(items=list(db.conversations.find(query, {'_id': 0}).sort('conversation_id', 1).skip((page - 1) * page_size).limit(page_size)),
                    page=page, page_size=page_size, total=db.conversations.count_documents(query))

    @app.post('/threads/open')
    def open_thread(body: ThreadOpen, user=Depends(actor)):
        booking_url = os.getenv('BOOKING_SERVICE_URL')
        if booking_url:
            token = jwt.encode({'sub': 'platform-service', 'role': 'service',
                                'exp': datetime.now(timezone.utc) + timedelta(minutes=5),
                                **({'iss': issuer} if issuer else {})}, secret, algorithm='HS256')
            try:
                response = httpx.get(booking_url.rstrip('/') + '/internal/reservations/' + body.reservation_id,
                                     headers={'Authorization': 'Bearer ' + token}, timeout=5)
            except httpx.HTTPError:
                raise ApiError(503, 'DEPENDENCY_UNAVAILABLE', 'Booking service unavailable.') from None
            if response.status_code == 404:
                raise ApiError(404, 'RESERVATION_NOT_FOUND', 'Unknown reservation.')
            if response.status_code != 200:
                raise ApiError(503, 'DEPENDENCY_UNAVAILABLE', 'Booking lookup failed.')
            reservation = response.json()
            source = 'booking-service'
        else:
            reservation = next((r for r in json.loads((ROOT / 'data/seed/generated/reservations.json').read_text())
                                if r['reservation_id'] == body.reservation_id), None)
            source = 'fixed-seed-pool'
        if not reservation:
            raise ApiError(404, 'RESERVATION_NOT_FOUND', 'Unknown reservation; configure BOOKING_SERVICE_URL for live bookings.')
        if user['role'] not in ('guest', 'host') or user['sub'] not in (reservation['guest_id'], reservation['host_id']):
            raise ApiError(403, 'FORBIDDEN', 'Reservation participants only.')
        existing = db.conversations.find_one({'reservation_id': body.reservation_id}, {'_id': 0})
        if existing:
            return {**existing, 'thread_id': existing['conversation_id'], 'source': source}
        cid = 'conv-' + body.reservation_id
        db.conversations.update_one({'conversation_id': cid}, {'$setOnInsert': dict(conversation_id=cid,
                                    reservation_id=body.reservation_id, guest_id=reservation['guest_id'],
                                    host_id=reservation['host_id'], created_datetime=now())}, upsert=True)
        return {**db.conversations.find_one({'conversation_id': cid}, {'_id': 0}), 'thread_id': cid, 'source': source}

    @app.post('/threads/get')
    def thread_get(body: ThreadQuery, user=Depends(actor)):
        return {**authorized_conversation(body.thread_id, user), 'thread_id': body.thread_id}

    @app.post('/messages/list')
    def message_list(body: ThreadQuery, user=Depends(actor)):
        return conversation(ConversationQuery(conversation_id=body.thread_id, page=body.page, page_size=body.page_size), user)

    @app.post('/threads/byUser')
    def threads_by_user(body: Page, user=Depends(actor)):
        result = conversation_list(user, body.page, body.page_size)
        result['items'] = [{**r, 'thread_id': r['conversation_id']} for r in result['items']]
        return result

    @app.post('/events/ingest')
    def event_ingest(event: dict, user=Depends(internal), x_event_key: str | None = Header(default=None)):
        try:
            validate(event, key=x_event_key, check_key=x_event_key is not None)
            accepted = ingest(db, event)
        except PermanentError as exc:
            raise ApiError(400 if exc.code == 'INVALID_EVENT_ENVELOPE' else 422, exc.code, exc.message) from None
        return JSONResponse(dict(accepted=True, duplicate=not accepted, event_id=event['event_id'], trace_id=event['trace_id']),
                            status_code=202 if accepted else 200)

    def funnel(property_ids, days):
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat().replace('+00:00', 'Z')
        rows = list(db.event_traces.find({'payload.property_id': {'$in': property_ids}, 'timestamp': {'$gte': cutoff}}, {'_id': 0}))
        saved = [x for x in rows if x['event_type'] == 'property.saved']
        return dict(views=sum(x['event_type'] == 'property.viewed' for x in rows),
                    saves=sum(x['payload'].get('saved') is True for x in saved),
                    unsaves=sum(x['payload'].get('saved') is False for x in saved))

    @app.get('/analytics/host/{host_id}')
    def host(host_id: str, user=Depends(actor), days: int = Query(30, ge=1, le=365), property_id: str | None = None):
        if user['role'] not in ('service', 'admin') and not (user['role'] == 'host' and user['sub'] == host_id):
            raise ApiError(403, 'FORBIDDEN', 'Host can only read their own funnel.')
        pool = properties()
        owned = [p for p in pool if pool[p]['host_id'] == host_id]
        if not owned:
            raise ApiError(404, 'HOST_NOT_FOUND', 'Unknown host in fixed seed pool.')
        if property_id and property_id not in owned:
            raise ApiError(404, 'PROPERTY_NOT_FOUND', 'Property does not belong to this host.')
        return dict(host_id=host_id, days=days, as_of=now(), property_source='fixed-seed-pool',
                    funnel=funnel([property_id] if property_id else owned, days))

    @app.get('/analytics/property/{property_id}')
    def property_stats(property_id: str, user=Depends(actor), days: int = Query(30, ge=1, le=365)):
        prop = properties().get(property_id)
        if not prop:
            raise ApiError(404, 'PROPERTY_NOT_FOUND', 'Unknown property in fixed seed pool.')
        if user['role'] not in ('service', 'admin') and user['sub'] != prop['host_id']:
            raise ApiError(403, 'FORBIDDEN', 'Property owner only.')
        return dict(property_id=property_id, as_of=now(), funnel=funnel([property_id], days))

    @app.get('/analytics/platform')
    def platform(user=Depends(internal), include_lag: bool = False):
        latest = {}
        for row in db.event_traces.find({'event_type': 'booking.updated'}, {'_id': 0}):
            p = row['payload']
            if p['version'] > latest.get(p['reservation_id'], {}).get('version', 0):
                latest[p['reservation_id']] = p
        statuses = {}
        for p in latest.values():
            statuses[p['to_status']] = statuses.get(p['to_status'], 0) + 1
        dlq = db.event_traces.count_documents({'event_type': 'platform.dlq'})
        topics, traces = {}, {}
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat().replace('+00:00', 'Z')
        retries = 0
        for e in db.event_traces.find({}, {'_id': 0}):
            topics[e['event_type']] = topics.get(e['event_type'], 0) + 1
            traces.setdefault(e['trace_id'], []).append(e['timestamp'])
            if e['event_type'] == 'platform.dlq':
                retries += e['payload'].get('retry_count', 0)
        durations = []
        for stamps in traces.values():
            if len(stamps) > 1:
                parsed = [datetime.fromisoformat(s.replace('Z', '+00:00')) for s in stamps]
                durations.append((max(parsed) - min(parsed)).total_seconds() * 1000)
        lag = {'status': 'not_requested', 'partitions': []}
        if include_lag:
            from .monitoring import consumer_lag
            lag = consumer_lag()
        return dict(as_of=now(), reservations=statuses,
                    messages={s: db.messages.count_documents({'delivery_status': s}) for s in ('queued', 'sent', 'failed')},
                    event_health={'accepted': db.event_traces.count_documents({'event_type': {'$ne': 'platform.dlq'}}),
                                  'rejected': dlq, 'dlq': dlq},
                    events_by_topic=topics, consumer_lag=lag,
                    received_records_per_second_last_60s=db.event_traces.count_documents({'_platform_received_at': {'$gte': cutoff}}) / 60,
                    failed_delivery_retry_count=retries,
                    mean_observed_trace_span_ms=sum(durations) / len(durations) if durations else None,
                    scope='Observed events only; trace span is max-min event timestamp, retries count DLQ failures only.')

    @app.get('/analytics/traces/{trace_id}')
    def trace_events(trace_id: str, user=Depends(internal)):
        return dict(trace_id=trace_id, items=list(db.event_traces.find({'trace_id': trace_id}, {'_id': 0}).sort('timestamp', 1)))

    @app.get('/analytics/funnel')
    def funnel_metrics(user=Depends(internal), days: int = Query(30, ge=1, le=365)):
        return dict(as_of=now(), days=days, funnel=funnel(list(properties()), days))

    @app.get('/analytics/properties/top')
    def top_properties(user=Depends(internal), days: int = Query(30, ge=1, le=365), limit: int = Query(10, ge=1, le=50)):
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat().replace('+00:00', 'Z')
        counts = {}
        for e in db.event_traces.find({'event_type': 'property.viewed', 'timestamp': {'$gte': cutoff}}):
            pid = e['payload']['property_id']
            counts[pid] = counts.get(pid, 0) + 1
        return dict(as_of=now(), items=[{'property_id': pid, 'views': n} for pid,n in sorted(counts.items(), key=lambda x: (-x[1], x[0]))[:limit]])

    @app.get('/analytics/geo')
    def geo(user=Depends(internal)):
        counts = {}
        pool = properties()
        for e in db.event_traces.find({'event_type': 'property.viewed'}):
            prop = pool.get(e['payload']['property_id'])
            if prop:
                city = prop.get('location', {}).get('city', 'unknown')
                counts[city] = counts.get(city, 0) + 1
        return dict(as_of=now(), scope='Observed views joined to fixed seed pool', items=[{'city': city, 'views': n} for city,n in sorted(counts.items())])

    @app.get('/analytics/host/dashboard')
    def host_dashboard(user=Depends(actor), host_id: str | None = None, days: int = Query(30, ge=1, le=365)):
        return host(host_id or user['sub'], user, days, None)

    @app.get('/analytics/guest/dashboard')
    def guest_dashboard(user=Depends(actor), days: int = Query(30, ge=1, le=365)):
        if user['role'] != 'guest':
            raise ApiError(403, 'FORBIDDEN', 'Guest token required.')
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat().replace('+00:00', 'Z')
        rows = list(db.event_traces.find({'payload.guest_id': user['sub'], 'timestamp': {'$gte': cutoff}}, {'_id': 0}))
        latest, statuses = {}, {}
        for e in rows:
            p = e['payload']
            if e['event_type'] == 'booking.updated' and p['version'] > latest.get(p['reservation_id'], {}).get('version', 0):
                latest[p['reservation_id']] = p
        for p in latest.values(): statuses[p['to_status']] = statuses.get(p['to_status'], 0) + 1
        return dict(as_of=now(), days=days, observed_bookings=len(latest), reservation_status_summary=statuses,
                    saved_property_activity=[e for e in rows if e['event_type'] == 'property.saved'], scope='Observed Kafka events only')

    # Static dashboard routes must precede /analytics/host/{host_id}.
    app.router.routes.sort(key=lambda route: '{' in getattr(route, 'path', ''))
    return app


app = create_app()
