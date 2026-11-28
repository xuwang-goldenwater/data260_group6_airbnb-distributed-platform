-- Role A rating projection; no existing listing or review data is replaced.
CREATE TABLE IF NOT EXISTS property_rating_state (
    property_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    rating_sum BIGINT UNSIGNED NOT NULL,
    review_count INT UNSIGNED NOT NULL,
    CONSTRAINT ck_rating_state_sum CHECK (
        (review_count = 0 AND rating_sum = 0) OR
        (review_count > 0 AND rating_sum BETWEEN review_count AND 5 * review_count)
    )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS property_rating_receipts (
    review_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL PRIMARY KEY,
    idempotency_key VARCHAR(256) COLLATE utf8mb4_bin NOT NULL,
    property_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    payload_hash CHAR(64) CHARACTER SET ascii NOT NULL,
    processed_datetime DATETIME(6) NOT NULL,
    UNIQUE KEY uq_property_rating_key (idempotency_key),
    KEY idx_rating_receipts_property (property_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS property_rating_bootstrap (
    singleton TINYINT UNSIGNED NOT NULL PRIMARY KEY,
    dataset_version VARCHAR(128) NOT NULL,
    reviews_sha256 CHAR(64) CHARACTER SET ascii NOT NULL,
    initialized_datetime DATETIME(6) NOT NULL,
    CONSTRAINT ck_rating_bootstrap_singleton CHECK (singleton = 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
