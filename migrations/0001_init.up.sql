-- Location history. The PDF names this table "vehicle_loctions" (likely a typo);
-- the correct spelling is used and a compatibility view is provided below.
CREATE TABLE vehicle_locations (
    id          BIGSERIAL PRIMARY KEY,
    vehicle_id  TEXT             NOT NULL,
    latitude    DOUBLE PRECISION NOT NULL CHECK (latitude  BETWEEN -90  AND 90),
    longitude   DOUBLE PRECISION NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    timestamp   BIGINT           NOT NULL CHECK (timestamp > 0), -- Unix seconds
    received_at TIMESTAMPTZ      NOT NULL DEFAULT now(),
    -- Makes MQTT at-least-once redelivery harmless (INSERT ... ON CONFLICT DO NOTHING) and
    -- doubles as the index for "latest" (scan backwards) and "history" (range scan).
    CONSTRAINT vehicle_locations_vehicle_ts_key UNIQUE (vehicle_id, timestamp)
);

CREATE VIEW vehicle_loctions AS
    SELECT vehicle_id, latitude, longitude, timestamp FROM vehicle_locations;

-- Last known geofence state per vehicle; row-locked while a reading is evaluated.
CREATE TABLE vehicle_geofence_state (
    vehicle_id     TEXT PRIMARY KEY,
    inside         BOOLEAN     NOT NULL,
    last_timestamp BIGINT      NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Transactional outbox: written in the same transaction as the state change,
-- published to RabbitMQ by a relay afterwards.
CREATE TABLE geofence_outbox (
    id           BIGSERIAL PRIMARY KEY,
    message_id   TEXT        NOT NULL UNIQUE,
    -- TEXT, not JSONB: JSONB reorders keys, and the message body must keep the exact
    -- field order and formatting given in the spec.
    payload      TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX geofence_outbox_unpublished_idx ON geofence_outbox (id) WHERE published_at IS NULL;
