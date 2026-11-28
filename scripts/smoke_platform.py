"""Live HTTP → Mongo outbox → Kafka → Analytics smoke test; isolated DB, real services."""
import json
import os
import secrets
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import httpx
import jwt
from pymongo import MongoClient

from platform_service.store import initialize, seed
from platform_service.worker import Publisher
from shared.consumer import build, now


def main():
    run = uuid.uuid4().hex[:12]
    db = MongoClient(os.getenv('MONGO_URI', 'mongodb://localhost:27017'))['platform_smoke_' + run]
    initialize(db)
    seed(db)
    secret = secrets.token_hex(32)
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
    env = {**os.environ, 'JWT_SECRET': secret, 'MONGO_DATABASE': db.name,
           'PLATFORM_CONSUMER_GROUP': 'platform-smoke-' + run}
    env.pop('JWT_ISSUER', None)
    children = []
    def wait_until(fn, seconds=30):
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            try:
                result = fn()
                if result: return result
            except httpx.HTTPError:
                pass
            time.sleep(.2)
        raise AssertionError('Timed out waiting for live service')
    try:
        children.append(subprocess.Popen([sys.executable, '-m', 'uvicorn', 'platform_service.app:app', '--port', str(port)], env=env))
        children.append(subprocess.Popen([sys.executable, '-m', 'platform_service.worker'], env=env))
        http = httpx.Client(base_url=f'http://127.0.0.1:{port}', timeout=10)
        wait_until(lambda: http.get('/health').status_code == 200)
        def auth(sub, role):
            return {'Authorization': 'Bearer ' + jwt.encode({'sub': sub, 'role': role, 'exp': int(time.time()) + 300}, secret, algorithm='HS256')}
        conv = db.conversations.find_one({'guest_id': 'guest-1001'})
        payload = {'conversation_id': conv['conversation_id'], 'recipient_id': conv['host_id'], 'body': 'Live platform smoke test ' + run}
        headers = {**auth('guest-1001', 'guest'), 'Idempotency-Key': run}
        sent = http.post('/messages/send', json=payload, headers=headers)
        assert sent.status_code == 201, sent.text
        message = sent.json()
        replay = http.post('/messages/send', json=payload, headers=headers)
        assert replay.status_code == 200 and replay.json()['message_id'] == message['message_id']
        wait_until(lambda: db.messages.find_one({'message_id': message['message_id'], 'delivery_status': 'sent'}))
        wait_until(lambda: db.event_traces.find_one({'event_type': 'message.created', 'payload.message_id': message['message_id']}))
        publisher = Publisher(os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'))
        view = build('property.viewed', 'guest-1001', 'property', 'property-2001', {'property_id': 'property-2001'}, 'smoke-view-' + run)
        publisher('property.viewed', 'property-2001', view)
        publisher('property.viewed', 'property-2001', view)
        wait_until(lambda: db.event_traces.find_one({'event_id': view['event_id']}))
        assert db.event_traces.count_documents({'event_id': view['event_id']}) == 1
        funnel = http.get('/analytics/host/host-501', headers=auth('booking-service', 'service'))
        assert funnel.status_code == 200 and funnel.json()['funnel']['views'] >= 1, funnel.text
        assert http.get('/analytics/platform', headers=auth('guest-1001', 'guest')).status_code == 403
        assert http.get('/platform').status_code == 200
        report = dict(measured_at=now(), database=db.name, http_send=201, http_replay=200,
                      message_outbox_delivered=True, kafka_message_ingested=True, duplicate_view_count=1,
                      booking_host_funnel_http=200, guest_platform_access=403, dashboard_http=200)
        output = Path('reports/perf/kafka/smoke.json')
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(report, indent=2))
    finally:
        for process in children:
            process.terminate()
        for process in children:
            try: process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


if __name__ == '__main__':
    main()
