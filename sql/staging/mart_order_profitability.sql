/*
mart_order_profitability.sql

Purpose:
Order-level profitability / reconciliation mart at grain = 1 row per order.

Grain:
1 row = 1 order_id

Inputs:
- fact_order
- fact_payment_settlement

Important:
We aggregate settlement lines to order grain before joining to avoid double counting.
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

        -- app-side price components
        o.order_value_app,
        o.delivery_fee,
        o.small_order_surcharge,
        o.discount_amount,

        (o.order_value_app
         + COALESCE(o.delivery_fee, 0)
         + COALESCE(o.small_order_surcharge, 0)
         - COALESCE(o.discount_amount, 0)
        )::numeric AS app_total_amount

    FROM fact_order o
),

settlement_agg AS (
    SELECT
        p.order_id,

        -- what we actually received from processor for this order (excluding refunds)
        SUM(CASE WHEN p.is_refund = FALSE THEN COALESCE(p.net_amount, 0) ELSE 0 END) AS settled_net_amount,

        -- refunds that are matched to the order_id (only linkable ones)
        SUM(CASE WHEN p.is_refund = TRUE THEN COALESCE(p.net_amount, 0) ELSE 0 END) AS refund_net_amount,

        -- quality signals
        COUNT(*) FILTER (WHERE p.match_status = 'MATCHED') AS matched_settlement_lines,
        COUNT(*) FILTER (WHERE p.match_status <> 'MATCHED') AS unmatched_settlement_lines

    FROM fact_payment_settlement p
    WHERE p.order_id IS NOT NULL
    GROUP BY p.order_id
),

final AS (
    SELECT
        ob.*,

        sa.settled_net_amount,
        sa.refund_net_amount,
        sa.matched_settlement_lines,
        sa.unmatched_settlement_lines,

        -- reconciliation: compare what app thinks vs what processor settled (non-refund)
        CASE
            WHEN sa.settled_net_amount IS NULL THEN NULL
            ELSE (ob.app_total_amount - sa.settled_net_amount)
        END AS app_vs_settlement_net_diff,

        -- placeholder for future costs (driver payouts, surge bonuses, support credits)
        NULL::numeric AS driver_payout_amount,
        NULL::numeric AS driver_surge_bonus_amount,

        -- provisional net revenue (will be refined once payouts + refunds linking improve)
        (
            COALESCE(sa.settled_net_amount, 0)
            + COALESCE(sa.refund_net_amount, 0) -- refund_net_amount is negative in many processors; if positive, we’ll standardize later
            - COALESCE(NULL::numeric, 0)        -- driver payout placeholder
        )::numeric AS net_revenue_provisional

    FROM order_base ob
    LEFT JOIN settlement_agg sa
      ON sa.order_id = ob.order_id
)

SELECT *
FROM final;
