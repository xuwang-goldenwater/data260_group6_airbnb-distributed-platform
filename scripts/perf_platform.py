"""Real Kafka/Mongo benchmark, separate ephemeral topic/group/DB for each run; no mocks.

Retains isolated benchmark databases and topics for inspection. Never deletes team data.
"""
import csv
import json
import os
import platform
import statistics
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from pymongo import MongoClient

from shared.consumer import RecordMeta, build, now, process_record
from platform_service.store import initialize, ingest
from platform_service.worker import Publisher

OUT = Path('reports/perf/kafka')
BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
MONGO = os.getenv('MONGO_URI', 'mongodb://localhost:27017')


def measure(workers, count=600):
    run = uuid.uuid4().hex[:12]
    # Production topic name is used in record metadata for strict type validation.
    topic, group = f'platform-bench-{run}', f'platform-bench-{run}'
    admin = AdminClient({'bootstrap.servers': BOOTSTRAP})
    admin.create_topics([NewTopic(topic, 3, 1)])[topic].result()
    db = MongoClient(MONGO)[f'platform_bench_{run}']
    initialize(db)
    lock, start, done = threading.Lock(), threading.Event(), threading.Event()
    assigned, seen, latencies, errors, assignments = set(), set(), [], [], {}
    publisher = Publisher(BOOTSTRAP)

    def worker(index):
        consumer = Consumer({'bootstrap.servers': BOOTSTRAP, 'group.id': group, 'enable.auto.commit': False,
                             'auto.offset.reset': 'earliest', 'session.timeout.ms': 6000})
        def on_assign(c, partitions):
            with lock:
                assigned.add(index)
                assignments[index] = [p.partition for p in partitions]
        consumer.subscribe([topic], on_assign=on_assign)
        deadline = time.monotonic() + 60
        try:
            while not done.is_set() and time.monotonic() < deadline:
                msg = consumer.poll(.1)
                if msg is None: continue
                if msg.error(): raise RuntimeError(str(msg.error()))
                meta = RecordMeta('property.viewed', msg.partition(), msg.offset(), msg.key().decode())
                process_record(msg.value(), meta, lambda e: ingest(db, e), publisher,
                               lambda: consumer.commit(message=msg, asynchronous=False),
                               group=group, is_transient=lambda e: False)
                with lock:
                    seen.add((msg.partition(), msg.offset()))
                    if len(seen) >= count + 3: done.set()
        except Exception as exc:
            errors.append(repr(exc))
            done.set()
        finally:
            consumer.close()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(worker, i) for i in range(workers)]
        deadline = time.monotonic() + 25
        while len(assigned) < workers and time.monotonic() < deadline:
            time.sleep(.1)
        if len(assigned) != workers: raise RuntimeError('Consumers did not receive assignments')
        begin = time.perf_counter()
        for i in range(count):
            e = build('property.viewed', 'guest-1001', 'property', f'p-{i % 30}',
                      {'property_id': f'p-{i % 30}'}, f'bench-{run}-{i}')
            publisher(topic, e['payload']['property_id'], e)
        # Three bad JSON records must go to the actual platform.dlq topic.
        for _ in range(3):
            publisher.producer.produce(topic, key='bad', value=b'not-json')
        if publisher.producer.flush(10): raise RuntimeError('Failed to publish invalid fixtures')
        if not done.wait(40): raise RuntimeError('Benchmark timed out')
        elapsed = time.perf_counter() - begin
        for future in futures: future.result()
    if errors: raise RuntimeError(errors)
    accepted = db.event_traces.count_documents({})
    # Verify actual DLQ records separately, without relying on an in-memory publish count.
    dlq = Consumer({'bootstrap.servers': BOOTSTRAP, 'group.id': group + '-verify', 'auto.offset.reset': 'earliest'})
    rejected, deadline = set(), time.monotonic() + 15
    dlq.subscribe(['platform.dlq'])
    try:
        while len(rejected) < 3 and time.monotonic() < deadline:
            m = dlq.poll(.2)
            if not m or m.error(): continue
            e = json.loads(m.value())
            if e['payload'].get('consumer_group') == group:
                rejected.add(e['idempotency_key'])
    finally:
        dlq.close()
    assert accepted == count and len(rejected) == 3, (accepted, rejected)
    return dict(workers=workers, records=count, rejected=len(rejected), elapsed_s=round(elapsed, 4),
                records_per_s=round(count / elapsed, 2), partitions=3, topic=topic,
                group=group, database=db.name, assignments=assignments)


if __name__ == '__main__':
    OUT.mkdir(parents=True, exist_ok=True)
    rows = [measure(n) for n in (1, 3)]
    report = {'measured_at': now(), 'machine': platform.platform(), 'cpu_count': os.cpu_count(),
              'method': '600 acknowledged publications, Mongo unique insert and synchronous offset commit; elapsed includes publishing, excludes group assignment. Single local broker, 3 partitions. One trial per configuration.',
              'results': rows}
    (OUT / 'results.json').write_text(json.dumps(report, indent=2) + '\n')
    with (OUT / 'throughput.csv').open('w') as f:
        writer = csv.DictWriter(f, fieldnames=['workers', 'records', 'rejected', 'elapsed_s', 'records_per_s'], lineterminator='\n')
        writer.writeheader()
        writer.writerows({k: r[k] for k in writer.fieldnames} for r in rows)
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    plt.bar([str(r['workers']) for r in rows], [r['records_per_s'] for r in rows], color='#164c63')
    plt.xlabel('Consumers in one group (3 partitions)')
    plt.ylabel('Validated Mongo writes / second')
    plt.title('Local Kafka → Mongo throughput (one trial)')
    plt.tight_layout()
    plt.savefig(OUT / 'throughput.png', dpi=160)
    print(json.dumps(report, indent=2))
