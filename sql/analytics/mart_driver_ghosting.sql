/*
mart_driver_ghosting.sql

Purpose:
Identify likely "ghosted" orders where a driver was assigned/accepted but did not move toward pickup.

Grain:
1 row = 1 order_id

Inputs:
- fact_order (order, driver_id, vendor_id)
- fact_order_status_event (needs ACCEPTED to be meaningful)
- stg_driver_gps_pings_deduped (driver movement)
- dim_vendor (optional: vendor location, neighborhood)

Definition (contract):
Ghosting candidate when:
1) order has driver_id
2) we have an ACCEPTED timestamp (Design A or future field)
3) within N minutes after ACCEPTED, driver shows:
   - no GPS pings at all, OR
   - very low movement distance (<= X meters), OR
   - no movement toward pickup area (future enhancement if vendor coords exist)

This is an approximate detection until we have vendor pickup coordinates.
*/

WITH accepted_events AS (
    SELECT
        order_id,
        MIN(status_ts) AS accepted_ts
    FROM fact_order_status_event
    WHERE status = 'ACCEPTED'
    GROUP BY order_id
),

orders AS (
    SELECT
        o.order_id,
        o.driver_id,
        o.vendor_id
    FROM fact_order o
    WHERE o.driver_id IS NOT NULL
),

-- GPS pings for the driver in a window after acceptance
gps_window AS (
    SELECT
        od.order_id,
        od.driver_id,
        ae.accepted_ts,
        p.ping_ts,
        p.latitude,
        p.longitude
    FROM orders od
    JOIN accepted_events ae
      ON ae.order_id = od.order_id
    LEFT JOIN stg_driver_gps_pings_deduped p
      ON p.driver_id = od.driver_id
     AND p.ping_ts >= ae.accepted_ts
     AND p.ping_ts <  ae.accepted_ts + INTERVAL '10 minutes'
),

-- approximate movement: distance between first and last ping in the window
movement AS (
    SELECT
        order_id,
        driver_id,
        accepted_ts,
        COUNT(ping_ts) AS ping_count,
        MIN(ping_ts) AS first_ping_ts,
        MAX(ping_ts) AS last_ping_ts,

        -- first/last coordinates in the window
        (ARRAY_AGG(latitude  ORDER BY ping_ts ASC))[1]  AS first_lat,
        (ARRAY_AGG(longitude ORDER BY ping_ts ASC))[1]  AS first_lon,
        (ARRAY_AGG(latitude  ORDER BY ping_ts DESC))[1] AS last_lat,
        (ARRAY_AGG(longitude ORDER BY ping_ts DESC))[1] AS last_lon

    FROM gps_window
    GROUP BY order_id, driver_id, accepted_ts
),
movement_scored AS (
    SELECT
        m.*,

        CASE
            WHEN m.ping_count <= 1 THEN 0::numeric
            ELSE
                (
                    2 * 6371000 * asin(  -- earth radius in meters
                        sqrt(
                            power(sin(radians((m.last_lat - m.first_lat) / 2)), 2)
                            +
                            cos(radians(m.first_lat)) * cos(radians(m.last_lat))
                            * power(sin(radians((m.last_lon - m.first_lon) / 2)), 2)
                        )
                    )
                )::numeric
        END AS movement_meters

    FROM movement m
)


final AS (
    SELECT
        m.order_id,
        m.driver_id,
        m.accepted_ts,
        m.ping_count,
        m.first_ping_ts,
        m.last_ping_ts,
        m.movement_proxy,

        CASE
            WHEN m.accepted_ts IS NULL THEN 'NO_ACCEPTED_EVENT'
            WHEN m.ping_count = 0 THEN 'NO_GPS_AFTER_ACCEPTED'
            WHEN m.ping_count = 1 THEN 'ONLY_ONE_GPS_PING'
            WHEN m.movement_meters < 50 THEN 'LOW_MOVEMENT_<50M'
            ELSE 'MOVED'
        END AS ghosting_status,

        CASE
            WHEN m.accepted_ts IS NULL THEN NULL
            WHEN m.ping_count = 0 THEN TRUE
            WHEN m.ping_count = 1 THEN TRUE
            WHEN m.movement_proxy < 50 THEN TRUE
            ELSE FALSE
        END AS is_ghosting_candidate

    FROM movement_scored m
)

SELECT *
FROM final;
