/*
mart_ops_fulfillment.sql

Purpose:
Operations fulfillment mart for monitoring delivery performance at grain = 1 row per order.

Grain:
1 row = 1 order_id

Inputs:
- fact_order
- fact_order_status_event (Design A preferred; Design B fallback)
- dim_vendor (for neighborhood, cuisine, etc.)

What this can support with Design B (milestones only):
- delivery cycle time (created -> delivered)
- late delivery flag (> 45 mins)
- cancellation timing (created -> cancelled)

What requires Design A (true status events):
- kitchen time (COOKING -> READY)
- driver wait time (ARRIVED_AT_VENDOR -> PICKED_UP)
- cancellation after food prepared (READY -> CANCELLED)
*/

WITH order_base AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.vendor_id,
        o.driver_id,
        o.created_at,
        o.delivered_at,
        o.cancelled_at,
        o.status_current,
        o.is_cancelled,
        o.is_delivered
    FROM fact_order o
),

vendor_current AS (
    -- current vendor attributes only (ops cares about current mapping; history can be used later)
    SELECT
        v.vendor_id,
        v.neighborhood_canonical,
        v.cuisine_type
    FROM dim_vendor v
    WHERE v.is_current = TRUE
),

events AS (
    SELECT
        e.order_id,

        -- milestone timestamps (available in Design B)
        MIN(CASE WHEN e.status = 'PLACED' THEN e.status_ts END)    AS placed_ts,
        MIN(CASE WHEN e.status = 'DELIVERED' THEN e.status_ts END) AS delivered_ts,
        MIN(CASE WHEN e.status = 'CANCELLED' THEN e.status_ts END) AS cancelled_ts

        -- If Design A exists later, add these:
        -- MIN(CASE WHEN e.status = 'READY' THEN e.status_ts END) AS ready_ts,
        -- MIN(CASE WHEN e.status = 'ARRIVED_AT_VENDOR' THEN e.status_ts END) AS arrived_vendor_ts,
        -- MIN(CASE WHEN e.status = 'PICKED_UP' THEN e.status_ts END) AS picked_up_ts

    FROM fact_order_status_event e
    GROUP BY e.order_id
),

final AS (
    SELECT
        ob.order_id,
        ob.customer_id,
        ob.vendor_id,
        ob.driver_id,

        vc.neighborhood_canonical,
        vc.cuisine_type,

        -- choose event timestamps if available, else fall back to fact_order timestamps
        COALESCE(ev.placed_ts, ob.created_at) AS placed_ts,
        COALESCE(ev.delivered_ts, ob.delivered_at) AS delivered_ts,
        COALESCE(ev.cancelled_ts, ob.cancelled_at) AS cancelled_ts,

        -- delivery cycle time (minutes)
        CASE
            WHEN COALESCE(ev.delivered_ts, ob.delivered_at) IS NOT NULL
                 AND COALESCE(ev.placed_ts, ob.created_at) IS NOT NULL
            THEN EXTRACT(EPOCH FROM (COALESCE(ev.delivered_ts, ob.delivered_at) - COALESCE(ev.placed_ts, ob.created_at))) / 60.0
            ELSE NULL
        END AS delivery_cycle_minutes,

        -- late delivery flag (>45 mins)
        CASE
            WHEN COALESCE(ev.delivered_ts, ob.delivered_at) IS NOT NULL
                 AND COALESCE(ev.placed_ts, ob.created_at) IS NOT NULL
                 AND (EXTRACT(EPOCH FROM (COALESCE(ev.delivered_ts, ob.delivered_at) - COALESCE(ev.placed_ts, ob.created_at))) / 60.0) > 45
            THEN TRUE
            ELSE FALSE
        END AS is_late_delivery_45m,

        -- cancellation timing (minutes from placed)
        CASE
            WHEN COALESCE(ev.cancelled_ts, ob.cancelled_at) IS NOT NULL
                 AND COALESCE(ev.placed_ts, ob.created_at) IS NOT NULL
            THEN EXTRACT(EPOCH FROM (COALESCE(ev.cancelled_ts, ob.cancelled_at) - COALESCE(ev.placed_ts, ob.created_at))) / 60.0
            ELSE NULL
        END AS cancel_after_minutes,

        -- placeholders for Design A metrics (filled once audit log exists)
        NULL::numeric AS kitchen_time_minutes,
        NULL::numeric AS driver_wait_time_minutes,
        NULL::boolean AS cancelled_after_food_prepared

    FROM order_base ob
    LEFT JOIN vendor_current vc
      ON vc.vendor_id = ob.vendor_id
    LEFT JOIN events ev
      ON ev.order_id = ob.order_id
)

SELECT *
FROM final;
