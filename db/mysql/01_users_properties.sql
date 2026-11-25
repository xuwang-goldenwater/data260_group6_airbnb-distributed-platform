-- Role A: User and Property schema. Target: MySQL 8.4 / InnoDB.
-- Run against the existing airbnb database with a UTF-8 client.
-- This creates missing tables; it does NOT migrate existing table definitions.
-- No DROP, TRUNCATE, seed regeneration, or Booking-owned tables are included.
--
-- API mapping:
--   property.location.* -> properties address/geographic columns below.
--   property.amenities[] -> amenities + property_amenities.
--   guest.preferences -> JSON object (nested fields validated by the service).
--   guest deletion -> deleted_datetime; retain historical IDs and references.
-- UTC DATETIME values are written by the services/importer; JSON uses ISO 8601 Z.
-- IDs are opaque, case-sensitive ASCII, with room for prefixed ULIDs.
-- Cross-service host_id/guest_id references intentionally have no database FK:
-- owning services validate them through internal APIs per docs/entities.md.
-- Property-owned relationships use restrictive FKs; deletion is explicit.
-- Booking must use compatible ID types/collation if adding physical FKs.
--
-- This file does not implement API validation, authorization, HTTP idempotency,
-- Kafka deduplication/outbox, aggregate updates, or Redis invalidation.
-- Those require service code and, where needed, later operational migrations.

SET NAMES utf8mb4;
SET time_zone = '+00:00';

CREATE TABLE IF NOT EXISTS guests (
    guest_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(254) NOT NULL,
    phone VARCHAR(16) NULL,
    location VARCHAR(200) NULL,
    profile_photo_url TEXT NULL,
    preferences JSON NULL,
    saved_properties_count INT UNSIGNED NOT NULL DEFAULT 0,
    booking_count INT UNSIGNED NOT NULL DEFAULT 0,
    profile_created_datetime DATETIME(6) NOT NULL,
    updated_datetime DATETIME(6) NOT NULL,
    deleted_datetime DATETIME(6) NULL,
    PRIMARY KEY (guest_id),
    UNIQUE KEY uq_guests_email (email),
    CONSTRAINT ck_guests_first_name CHECK (CHAR_LENGTH(TRIM(first_name)) >= 1),
    CONSTRAINT ck_guests_last_name CHECK (CHAR_LENGTH(TRIM(last_name)) >= 1),
    CONSTRAINT ck_guests_preferences CHECK (
        preferences IS NULL OR JSON_TYPE(preferences) = 'OBJECT'
    )
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS hosts (
    host_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    name VARCHAR(150) NOT NULL,
    email VARCHAR(254) NOT NULL,
    phone VARCHAR(16) NULL,
    profile_photo_url TEXT NULL,
    verification_status VARCHAR(16) NOT NULL DEFAULT 'pending',
    response_rate DECIMAL(5,4) NOT NULL DEFAULT 0.0000,
    response_time INT UNSIGNED NULL,
    host_since DATE NOT NULL,
    access_level VARCHAR(16) NOT NULL DEFAULT 'host',
    updated_datetime DATETIME(6) NOT NULL,
    PRIMARY KEY (host_id),
    UNIQUE KEY uq_hosts_email (email),
    CONSTRAINT ck_hosts_name CHECK (CHAR_LENGTH(TRIM(name)) >= 1),
    CONSTRAINT ck_hosts_verification CHECK (
        verification_status IN ('pending', 'verified', 'rejected')
    ),
    CONSTRAINT ck_hosts_response_rate CHECK (response_rate BETWEEN 0 AND 1),
    CONSTRAINT ck_hosts_access_level CHECK (access_level IN ('host', 'admin'))
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS properties (
    property_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    host_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    title VARCHAR(150) NOT NULL,
    description TEXT NOT NULL,
    property_type VARCHAR(16) NOT NULL,
    address_line_1 VARCHAR(255) NOT NULL,
    city VARCHAR(200) NOT NULL,
    state VARCHAR(100) NOT NULL,
    postal_code VARCHAR(32) NOT NULL,
    country CHAR(2) NOT NULL,
    latitude DECIMAL(9,6) NOT NULL,
    longitude DECIMAL(10,6) NOT NULL,
    capacity TINYINT UNSIGNED NOT NULL,
    bedrooms TINYINT UNSIGNED NOT NULL,
    bathrooms DECIMAL(4,2) NOT NULL,
    nightly_price DECIMAL(10,2) NOT NULL,
    cleaning_fee DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    availability VARCHAR(24) NOT NULL DEFAULT 'available',
    status VARCHAR(16) NOT NULL DEFAULT 'draft',
    rating DECIMAL(5,4) NOT NULL DEFAULT 0.0000,
    review_count INT UNSIGNED NOT NULL DEFAULT 0,
    version BIGINT UNSIGNED NOT NULL DEFAULT 1,
    created_datetime DATETIME(6) NOT NULL,
    updated_datetime DATETIME(6) NOT NULL,
    PRIMARY KEY (property_id),
    KEY idx_properties_host (host_id, status, property_id),
    KEY idx_properties_search_city_price (status, city, nightly_price, property_id),
    CONSTRAINT ck_properties_title CHECK (CHAR_LENGTH(TRIM(title)) BETWEEN 5 AND 150),
    CONSTRAINT ck_properties_description CHECK (CHAR_LENGTH(description) BETWEEN 20 AND 5000),
    CONSTRAINT ck_properties_type CHECK (
        property_type IN ('apartment', 'house', 'condo', 'cabin', 'room')
    ),
    CONSTRAINT ck_properties_capacity CHECK (capacity BETWEEN 1 AND 30),
    CONSTRAINT ck_properties_bedrooms CHECK (bedrooms BETWEEN 0 AND 20),
    CONSTRAINT ck_properties_bathrooms CHECK (bathrooms BETWEEN 0.5 AND 20),
    CONSTRAINT ck_properties_price CHECK (nightly_price > 0),
    CONSTRAINT ck_properties_cleaning_fee CHECK (cleaning_fee >= 0),
    CONSTRAINT ck_properties_latitude CHECK (latitude BETWEEN -90 AND 90),
    CONSTRAINT ck_properties_longitude CHECK (longitude BETWEEN -180 AND 180),
    CONSTRAINT ck_properties_availability CHECK (
        availability IN ('available', 'partially_available', 'unavailable')
    ),
    CONSTRAINT ck_properties_status CHECK (
        status IN ('draft', 'active', 'suspended', 'archived')
    ),
    CONSTRAINT ck_properties_rating CHECK (rating BETWEEN 0 AND 5),
    CONSTRAINT ck_properties_version CHECK (version >= 1),
    CONSTRAINT ck_properties_empty_rating CHECK (review_count <> 0 OR rating = 0),
    CONSTRAINT ck_properties_nonempty_address CHECK (
        CHAR_LENGTH(TRIM(address_line_1)) > 0 AND CHAR_LENGTH(TRIM(city)) > 0
        AND CHAR_LENGTH(TRIM(state)) > 0 AND CHAR_LENGTH(TRIM(postal_code)) > 0
        AND CHAR_LENGTH(TRIM(country)) = 2
    )
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS amenities (
    amenity_name VARCHAR(100) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY (amenity_name),
    CONSTRAINT ck_amenities_normalized CHECK (
        CHAR_LENGTH(amenity_name) > 0
        AND BINARY amenity_name = BINARY LOWER(TRIM(amenity_name))
    )
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS property_amenities (
    property_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    amenity_name VARCHAR(100) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    PRIMARY KEY (property_id, amenity_name),
    KEY idx_property_amenities_lookup (amenity_name, property_id),
    CONSTRAINT fk_property_amenities_property FOREIGN KEY (property_id)
        REFERENCES properties (property_id) ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT fk_property_amenities_amenity FOREIGN KEY (amenity_name)
        REFERENCES amenities (amenity_name) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS saved_properties (
    guest_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    property_id VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    saved_datetime DATETIME(6) NOT NULL,
    PRIMARY KEY (guest_id, property_id),
    KEY idx_saved_properties_recent (guest_id, saved_datetime, property_id),
    KEY idx_saved_properties_property (property_id),
    CONSTRAINT fk_saved_properties_property FOREIGN KEY (property_id)
        REFERENCES properties (property_id) ON DELETE RESTRICT ON UPDATE RESTRICT
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

-- Importer order for this slice: guests, hosts, properties, amenities,
-- property_amenities, saved_properties. Split nested location and amenities
-- from the existing JSON; do not attempt to insert them as property columns.
-- Counts and aggregates come from a single consistent seed snapshot.
-- Keep validation of date-range availability in Booking, not this summary column.
