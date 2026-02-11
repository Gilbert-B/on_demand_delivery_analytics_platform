/*
stg_driver_gps_pings_deduped.sql

Purpose:
Deduplicate GPS pings caused by client retries / unstable networks.

Deduping strategy (deterministic):
Keep the earliest ingested row for each unique ping fingerprint.

Fingerprint:
(driver_id, ping_ts, latitude, longitude)

Output grain:
1 row per unique GPS ping event.
*/

WITH p AS (
    SELECT *
    FROM stg_driver_gps_pings_clean
),

ranked AS (
    SELECT
        p.*,
        ROUND(p.latitude, 5)  AS lat_r,
        ROUND(p.longitude, 5) AS lon_r,

        md5(
            concat_ws(
                '|',
                p.driver_id::text,
                p.ping_ts::text,
                ROUND(p.latitude, 5)::text,
                ROUND(p.longitude, 5)::text
            )
        ) AS ping_id,

        ROW_NUMBER() OVER (
            PARTITION BY p.driver_id, p.ping_ts, ROUND(p.latitude, 5), ROUND(p.longitude, 5)
            ORDER BY p.ingested_at ASC, p.source_file ASC, p.source_row_number ASC
        ) AS rn
    FROM p
)

SELECT
    ping_id,
    driver_id,
    ping_ts,
    event_date,
    latitude,
    longitude,
    battery_level,
    source_file,
    source_row_number,
    ingested_at
FROM ranked
WHERE rn = 1;
