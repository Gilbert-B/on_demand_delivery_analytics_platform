/*
mart_driver_behavior_weekly.sql

Purpose:
Weekly driver behavior scorecard combining:
- availability (from gps pings)
- ghosting candidates (from ghosting mart)

Grain:
1 row = (driver_id, week_start_date)

Inputs:
- mart_driver_availability_hourly
- mart_driver_ghosting

Definitions:
- active_hours: count of hours with is_active_in_hour = true
- ghosting_rate: ghosted_orders / orders_with_accepted_event
*/

WITH availability AS (
    SELECT
        driver_id,
        date_trunc('week', hour_ts)::date AS week_start_date,
        COUNT(*) FILTER (WHERE is_active_in_hour = TRUE) AS active_hours,
        SUM(ping_count) AS total_pings
    FROM mart_driver_availability_hourly
    GROUP BY 1, 2
),

ghosting AS (
    SELECT
        driver_id,
        date_trunc('week', accepted_ts)::date AS week_start_date,

        COUNT(*) FILTER (WHERE accepted_ts IS NOT NULL) AS orders_with_accepted_event,
        COUNT(*) FILTER (WHERE is_ghosting_candidate = TRUE) AS ghosted_orders

    FROM mart_driver_ghosting
    GROUP BY 1, 2
),

final AS (
    SELECT
        COALESCE(a.driver_id, g.driver_id) AS driver_id,
        COALESCE(a.week_start_date, g.week_start_date) AS week_start_date,

        COALESCE(a.active_hours, 0) AS active_hours,
        COALESCE(a.total_pings, 0) AS total_pings,

        COALESCE(g.orders_with_accepted_event, 0) AS orders_with_accepted_event,
        COALESCE(g.ghosted_orders, 0) AS ghosted_orders,

        CASE
            WHEN COALESCE(g.orders_with_accepted_event, 0) = 0 THEN NULL
            ELSE (g.ghosted_orders::numeric / g.orders_with_accepted_event::numeric)
        END AS ghosting_rate


        CASE
            WHEN COALESCE(a.active_hours, 0) >= 10 AND COALESCE(g.orders_with_accepted_event, 0) >= 5
                AND (g.ghosted_orders::numeric / NULLIF(g.orders_with_accepted_event::numeric, 0)) >= 0.2
              THEN 'HIGH_GHOSTING'
             WHEN COALESCE(a.active_hours, 0) = 0 AND COALESCE(g.orders_with_accepted_event, 0) = 0
                THEN 'INACTIVE'
            ELSE 'OK'
        END AS driver_week_status


    FROM availability a
    FULL OUTER JOIN ghosting g
      ON g.driver_id = a.driver_id
     AND g.week_start_date = a.week_start_date
)

SELECT *
FROM final;
