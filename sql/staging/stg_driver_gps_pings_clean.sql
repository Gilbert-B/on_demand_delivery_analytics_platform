/*
stg_driver_gps_pings_clean.sql

Purpose:
Clean raw GPS pings coming from S3 JSON logs.

Key problems handled:
- type normalization (lat/lon/timestamp/battery)
- keep ingestion metadata
- create event_date based on ping_ts (NOT ingested_at) to handle late-arriving data

Source (future):
raw.driver_gps_pings_json

Output (future):
stg_driver_gps_pings_clean
*/

WITH src AS (
    SELECT
        driver_id,
        ping_ts,          -- may be text in raw
        latitude,
        longitude,
        battery_level,

        -- ingestion metadata
        source_file,
        source_row_number,
        ingested_at
    FROM raw.driver_gps_pings_json
),

cleaned AS (
    SELECT
        driver_id,

        -- normalize timestamp
        (ping_ts::timestamptz) AS ping_ts,

        -- normalize coordinates
        latitude::numeric  AS latitude,
        longitude::numeric AS longitude,

        -- battery may be missing or text
        NULLIF(battery_level::text, '')::numeric AS battery_level,

        -- event partition key (handles late-arriving data correctly)
        (ping_ts::timestamptz)::date AS event_date,

        -- lineage
        source_file,
        source_row_number,
        ingested_at
    FROM src
)

SELECT *
FROM cleaned
WHERE ping_ts IS NOT NULL;
