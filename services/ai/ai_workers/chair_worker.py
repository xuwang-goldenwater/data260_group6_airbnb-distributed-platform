"""Kafka Chair process (Role D).

    PYTHONPATH=services/ai python -m ai_workers.chair_worker          # run from the repo root

Loop: poll one record -> validate envelope -> ChairCore.handle() (state saved to MongoDB inside)
      -> publish what it returns and wait for Kafka -> commit the offset.
Every half second ChairCore.tick() closes rounds whose deadline passed (agent timeout).

Order matters: save, then publish, then commit. A crash before the commit only causes a redelivery,
which the Chair recognises by idempotency_key, so nothing is lost and nothing is counted twice.
"""
from __future__ import annotations

import logging
import signal

from ai_core import envelope as env
from ai_core.chair_core import ChairCore
from ai_core.config import T_PARSED, T_ROUND0, T_ROUND2, settings
from ai_core.explanation import ollama_explainer, template_explainer
from ai_core.store import MongoStore

from .kafka_io import make_consumer, make_producer, publish, send_to_dlq

GROUP = "ai-chair"
log = logging.getLogger("ai.chair")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    s = settings()
    explainer = (ollama_explainer(s.ollama_url, s.explain_model, s.explain_timeout_s)
                 if s.explain_mode == "llm" else template_explainer)
    core = ChairCore(MongoStore(s.mongo_uri, s.mongo_db), round_timeout_s=s.round_timeout_s, explainer=explainer)
    log.info("explanation mode: %s", s.explain_mode)
    producer = make_producer(s, "ai-chair")
    log.info("recovered %d open debate(s) from MongoDB", core.recover())
    core.mark_sent(publish(producer, core.unsent()))         # finish anything cut off by a crash
    consumer = make_consumer(s, GROUP, [T_PARSED, T_ROUND0, T_ROUND2])

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        while running:
            msg = consumer.poll(0.5)
            if msg is not None and not msg.error():
                key = msg.key().decode() if msg.key() else None
                try:
                    event = env.decode(msg.value(), s.envelope_schema, key=key)
                    out = core.handle(event)
                    core.mark_sent(publish(producer, out))
                    if out:
                        log.info("%s %s -> %s", event["event_type"], event["trace_id"], [o[0] for o in out])
                except env.EnvelopeError as err:
                    send_to_dlq(producer, msg, GROUP, err)
                consumer.commit(message=msg, asynchronous=False)
            elif msg is not None:
                log.warning("kafka error: %s", msg.error())
            timed_out = core.tick()
            if timed_out:
                log.info("deadline closed round(s): %s", [o[1] for o in timed_out])
                core.mark_sent(publish(producer, timed_out))
    finally:
        consumer.close()
        producer.flush(5)


if __name__ == "__main__":
    main()
