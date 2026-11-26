"""Run the completion job once: confirmed reservations whose check_out has passed → completed.

Each transition writes one history row (actor completion-job) and publishes booking.updated.
BOOKING_CLOCK_DATE=YYYY-MM-DD can move "today" forward for a demo.
"""
from __future__ import annotations

import json
import logging

import _bootstrap  # noqa: F401
from booking_core.clients import AnalyticsClient, UserClient, make_property_client
from booking_core.config import Settings
from booking_core.db import Database
from booking_core.kafka_producer import KafkaEventProducer
from booking_core.service import BookingService


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [completion-job] %(message)s")
    s = Settings.from_env()
    producer = KafkaEventProducer(s, client_id="booking-completion-job")
    try:
        svc = BookingService(s, Database(s), producer, make_property_client(s), UserClient(s), AnalyticsClient(s))
        print(json.dumps(svc.complete_stays(), indent=2))
    finally:
        producer.close()


if __name__ == "__main__":
    main()
