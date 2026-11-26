"""One agent process: parser, matching, risk or fairness (wraps Role E's agent code).

    PYTHONPATH=services/ai python -m ai_workers.agent_worker matching           # from the repo root
    PYTHONPATH=services/ai python -m ai_workers.agent_worker fairness --delay-s 15   # slow agent demo

Each agent has its own consumer group (ai-agent-<name>), so every agent sees every message.
--delay-s makes the agent answer late, which is how the agent-timeout demo is produced.
--corrupt makes the agent send an invalid Round 0 (score 1.7, unknown property): the Chair's harness
must reject it and record a limitation instead of using it (invalid-output demo, task 5.4).
"""
from __future__ import annotations

import argparse
import logging
import signal
import time

from ai_core import agent_logic, envelope as env
from ai_core.config import T_PARSED, T_REQUESTS, T_ROUND1, settings

from .kafka_io import make_consumer, make_producer, publish, send_to_dlq

log = logging.getLogger("ai.agent")


def corrupt(out):
    """Break a Round 0 payload in two ways the harness must catch."""
    topic, key, event = out
    ranked = event["payload"].get("ranked_properties")
    if ranked:
        ranked[0] = {**ranked[0], "property_id": "property-9999", "score": 1.7}
    return topic, key, event


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("agent", choices=["parser", "matching", "risk", "fairness"])
    ap.add_argument("--delay-s", type=float, default=0.0, help="sleep before answering (timeout demo)")
    ap.add_argument("--corrupt", action="store_true", help="send invalid Round 0 output (harness demo)")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s {args.agent} %(message)s")

    s = settings()
    group = f"ai-agent-{args.agent}"
    topics = [T_REQUESTS] if args.agent == "parser" else [T_PARSED, T_ROUND1]
    consumer = make_consumer(s, group, topics)
    producer = make_producer(s, group)
    running = True

    def stop(*_):
        nonlocal running
        running = False

    # SIGTERM (run_demo, docker stop) must also leave the consumer group cleanly; otherwise the
    # next worker in this group waits up to session.timeout.ms for the dead member to expire
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        while running:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            key = msg.key().decode() if msg.key() else None
            try:
                event = env.decode(msg.value(), s.envelope_schema, key=key)
                if args.agent == "parser":
                    out = agent_logic.parser_step(event)
                elif event["event_type"] == T_PARSED:
                    out = agent_logic.round0_step(args.agent, event)
                else:
                    out = agent_logic.round2_step(args.agent, event)
                if args.corrupt:
                    out = [corrupt(o) for o in out]
                if out and args.delay_s:
                    time.sleep(args.delay_s)
                publish(producer, out)
                for topic, trace_id, _ in out:
                    log.info("%s -> %s", trace_id, topic)
            except env.EnvelopeError as err:
                send_to_dlq(producer, msg, group, err)
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
