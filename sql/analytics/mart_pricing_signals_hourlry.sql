/*
mart_pricing_signals_hourly.sql

Purpose:
Hourly pricing signals for dynamic pricing decisions using internal data only.

Grain:
1 row = (neighborhood_canonical, hour_ts)

Inputs:
- mart_ops_fulfillment (order cycle time + late flag)
- mart_driver_availability_hourly (active drivers)
- dim_vendor (neighborhood mapping)

Notes:
- This is not "true surge pricing" yet.
- It provides interpretable signals: demand vs supply and service degradation.
*/

WITH orders_hourly AS (
    SELECT
        date_trunc('hour', placed_ts) AS hour_ts,
        neighborhood_canonical,

        COUNT(*) AS orders_placed,
        COUNT(*) FILTER (WHERE is_late_delivery_45m = TRUE) AS late_orders,
        COUNT(*) FILTER (WHERE cancelled_ts IS NOT NULL) AS cancelled_orders,

        AVG(delivery_cycle_minutes) AS avg_delivery_cycle_minutes

    FROM mart_ops_fulfillment
    GROUP BY 1, 2
),

drivers_hourly AS (
    -- We don't have driver neighborhood yet, so treat this as global supply for now.
    -- In Phase 2, we’ll geo-assign drivers to neighborhoods using GPS clustering.
    SELECT
        hour_ts,
        COUNT(*) FILTER (WHERE is_active_in_hour = TRUE) AS active_drivers
    FROM mart_driver_availability_hourly
    GROUP BY 1
),

final AS (
    SELECT
        o.hour_ts,
        o.neighborhood_canonical,
        o.orders_placed,
        o.late_orders,
        o.cancelled_orders,
        o.avg_delivery_cycle_minutes,

        COALESCE(d.active_drivers, 0) AS active_drivers,

        CASE
            WHEN COALESCE(d.active_drivers, 0) = 0 THEN NULL
            ELSE (o.orders_placed::numeric / d.active_drivers::numeric)
        END AS orders_per_active_driver,

        CASE
            WHEN o.orders_placed < 5 THEN 'LOW_VOLUME'
            WHEN COALESCE(d.active_drivers, 0) = 0 THEN 'NO_SUPPLY'
            WHEN (o.late_orders::numeric / NULLIF(o.orders_placed::numeric, 0)) >= 0.30 THEN 'SERVICE_DEGRADED'
            WHEN (o.orders_placed::numeric / NULLIF(d.active_drivers::numeric, 0)) >= 2.0 THEN 'DRIVER_SCARCITY'
            ELSE 'NORMAL'
        END AS surge_signal

    FROM orders_hourly o
    LEFT JOIN drivers_hourly d
      ON d.hour_ts = o.hour_ts
)

SELECT *
FROM final;
