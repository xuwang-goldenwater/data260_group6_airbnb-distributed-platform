-- Role A: durable HTTP idempotency and transactional property-event outbox.
-- Does not change/delete existing User, Property, or Booking records.
CREATE TABLE IF NOT EXISTS property_http_operations (
    actor_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
    endpoint VARCHAR(64) NOT NULL,
    idempotency_key CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    request_hash CHAR(64) CHARACTER SET ascii NOT NULL,
    response_body JSON NULL,
    created_datetime DATETIME(6) NOT NULL,
    PRIMARY KEY (actor_id, endpoint, idempotency_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS property_event_outbox (
    outbox_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    event_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    topic VARCHAR(100) NOT NULL,
    property_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    envelope JSON NOT NULL,
    created_datetime DATETIME(6) NOT NULL,
    published_datetime DATETIME(6) NULL,
    publish_attempts INT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (outbox_id),
    UNIQUE KEY uq_property_outbox_event (event_id),
    KEY idx_property_outbox_pending (published_datetime, outbox_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
