-- Role B — Booking & Availability (owner: Ayushi Shringi)
-- Tables owned by the Booking service. Contract: docs/api/booking.md "Data Model (MySQL)".
-- Idempotent: safe to apply repeatedly (CREATE TABLE IF NOT EXISTS). Timestamps are UTC DATETIME(3).
-- property_id / guest_id / host_id / actor_id use VARCHAR(64) ascii_bin, the same type as db/mysql/01_users_properties.sql,
-- so they join with Role A's tables without a collation conversion. No cross-service FK (docs/entities.md).

CREATE TABLE IF NOT EXISTS reservations (
  reservation_id    VARCHAR(40)    NOT NULL,
  property_id       VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin    NOT NULL,
  guest_id          VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin    NOT NULL,
  host_id           VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin    NOT NULL,
  check_in          DATE           NOT NULL,
  check_out         DATE           NOT NULL,
  nights            INT            NOT NULL,
  guest_count       INT            NOT NULL,
  nightly_price     DECIMAL(10,2)  NOT NULL,
  cleaning_fee      DECIMAL(10,2)  NOT NULL,
  total_price       DECIMAL(10,2)  NOT NULL,
  status            ENUM('requested','pending_host','confirmed','rejected','declined','cancelled','completed') NOT NULL,
  status_reason     VARCHAR(64)    NULL,
  message_to_host   VARCHAR(1000)  NULL,
  cancelled_by      ENUM('guest','host') NULL,
  trace_id          VARCHAR(40)    NOT NULL,
  publish_pending   BOOLEAN        NOT NULL DEFAULT FALSE,
  version           INT            NOT NULL DEFAULT 1,
  created_datetime  DATETIME(3)    NOT NULL,
  updated_datetime  DATETIME(3)    NOT NULL,
  PRIMARY KEY (reservation_id),
  KEY idx_res_guest_created (guest_id, created_datetime),
  KEY idx_res_host_status_created (host_id, status, created_datetime),
  KEY idx_res_property_checkin (property_id, check_in),
  KEY idx_res_status_checkout (status, check_out),
  KEY idx_res_publish_pending (publish_pending, updated_datetime),
  CONSTRAINT chk_res_dates CHECK (check_out > check_in),
  CONSTRAINT chk_res_nights CHECK (nights >= 1 AND nights = DATEDIFF(check_out, check_in)),
  CONSTRAINT chk_res_guest_count CHECK (guest_count >= 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- The double-booking guard: one row per held night. The primary key makes it physically
-- impossible to store two reservations for the same night of the same property.
CREATE TABLE IF NOT EXISTS availability_blocks (
  property_id       VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin  NOT NULL,
  night_date        DATE         NOT NULL,
  reservation_id    VARCHAR(40)  NOT NULL,
  created_datetime  DATETIME(3)  NOT NULL,
  PRIMARY KEY (property_id, night_date),
  KEY idx_blocks_reservation (reservation_id),
  CONSTRAINT fk_blocks_reservation FOREIGN KEY (reservation_id) REFERENCES reservations (reservation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS reservation_status_history (
  id                BIGINT       NOT NULL AUTO_INCREMENT,
  reservation_id    VARCHAR(40)  NOT NULL,
  from_status       ENUM('requested','pending_host','confirmed','rejected','declined','cancelled','completed') NULL,
  to_status         ENUM('requested','pending_host','confirmed','rejected','declined','cancelled','completed') NOT NULL,
  actor_id          VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin  NOT NULL,
  reason            VARCHAR(64)  NULL,
  trace_id          VARCHAR(40)  NOT NULL,
  changed_datetime  DATETIME(3)  NOT NULL,
  PRIMARY KEY (id),
  KEY idx_hist_reservation (reservation_id, changed_datetime),
  KEY idx_hist_to_status_changed (to_status, changed_datetime),
  CONSTRAINT fk_hist_reservation FOREIGN KEY (reservation_id) REFERENCES reservations (reservation_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- REST Idempotency-Key store. Rows older than 24 h are purged by the API's background job.
CREATE TABLE IF NOT EXISTS idempotency_keys (
  idempotency_key   VARCHAR(64)  NOT NULL,
  actor_id          VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin  NOT NULL,
  endpoint          VARCHAR(64)  NOT NULL,
  request_hash      CHAR(64)     NOT NULL,
  response_code     INT          NOT NULL,
  response_body     JSON         NOT NULL,
  created_datetime  DATETIME(3)  NOT NULL,
  PRIMARY KEY (actor_id, endpoint, idempotency_key),
  KEY idx_idem_created (created_datetime)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Kafka consumer de-duplication (one row per processed event idempotency_key).
CREATE TABLE IF NOT EXISTS processed_events (
  idempotency_key     VARCHAR(128) NOT NULL,
  consumer            VARCHAR(64)  NOT NULL,
  processed_datetime  DATETIME(3)  NOT NULL,
  PRIMARY KEY (idempotency_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
