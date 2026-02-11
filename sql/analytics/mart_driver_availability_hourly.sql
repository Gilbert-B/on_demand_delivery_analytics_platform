/*
mart_driver_availability_hourly.sql

Purpose:
Approximate driver availability using GPS pings.
Grain:
1 row = (hour_ts, driver_id)

Definition (approx):
A driver is considered "active" in an hour if we have >= 1 deduped ping in that hour.

Inputs:
- stg_driver_gps_pings_deduped

Notes:
- This measures "ping presence", not true app online status.
- Late-arriving data is handled because event_date is derived from ping_ts.
*/

WITH p AS (
    SELECT
        driver_id,
        ping_ts,
        latitude,
        longitude,
        event_date
    FROM stg_driver_gps_pings_deduped
),

hourly AS (
    SELECT
        date_trunc('hour', ping_ts) AS hour_ts,
        driver_id,

        COUNT(*) AS ping_count,
        MIN(ping_ts) AS first_ping_ts,
        MAX(ping_ts) AS last_ping_ts

    FROM p
    GROUP BY 1, 2
)

SELECT
    hour_ts,
    driver_id,
    ping_count,
    first_ping_ts,
    last_ping_ts,

    CASE
        WHEN ping_count >= 1 THEN TRUE
        ELSE FALSE
    END AS is_active_in_hour

FROM hourly;
