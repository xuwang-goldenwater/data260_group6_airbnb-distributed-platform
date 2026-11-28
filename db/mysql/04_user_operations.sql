-- Role A: durable profile-write idempotency. No existing table/data is replaced.
CREATE TABLE IF NOT EXISTS user_http_operations (
    actor_id VARCHAR(128) COLLATE utf8mb4_bin NOT NULL,
    endpoint VARCHAR(64) NOT NULL,
    idempotency_key CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    request_hash CHAR(64) CHARACTER SET ascii NOT NULL,
    response_body JSON NULL,
    created_datetime DATETIME(6) NOT NULL,
    PRIMARY KEY (actor_id, endpoint, idempotency_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
