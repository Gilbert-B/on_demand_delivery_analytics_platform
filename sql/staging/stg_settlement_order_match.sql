/*
stg_settlement_order_match.sql

Purpose:
Best-effort matching of settlement lines to app orders and compute discrepancy helpers.

Inputs (future):
- stg_settlement_clean
- public.orders (later: fact_order)

Output (future):
- stg_settlement_order_match
*/

WITH s AS (
    SELECT *
    FROM stg_settlement_clean
),

o AS (
    SELECT
        order_id,
        (subtotal_amount
         + delivery_fee
         + COALESCE(small_order_surcharge, 0)
         - COALESCE(discount_amount, 0)
        )::numeric AS app_total_amount
    FROM public.orders
),

matched AS (
    SELECT
        s.settlement_line_id,
        s.settlement_date,
        s.reference_raw,
        s.reference_clean,
        s.transaction_id,
        s.transaction_type,
        s.currency,
        s.gross_amount,
        s.processor_fee,
        s.net_amount,
        s.is_refund,

        o.order_id,
        o.app_total_amount,

        CASE
            WHEN s.reference_clean IS NULL THEN 'NO_REFERENCE'
            WHEN o.order_id IS NULL THEN 'NO_ORDER_MATCH'
            ELSE 'MATCHED'
        END AS match_status,

        CASE
            WHEN o.order_id IS NOT NULL AND s.is_refund = FALSE
                THEN (o.app_total_amount - s.gross_amount)
            ELSE NULL
        END AS gross_discrepancy_amount,

        CASE
            WHEN o.order_id IS NOT NULL AND s.is_refund = FALSE
                THEN (o.app_total_amount - s.net_amount)
            ELSE NULL
        END AS net_discrepancy_amount,

        CASE
            WHEN s.reference_clean IS NOT NULL AND o.order_id IS NOT NULL THEN 'HIGH'
            ELSE 'NONE'
        END AS match_confidence,


        s.source_file,
        s.source_row_number,
        s.ingested_at

    FROM s
    LEFT JOIN o
      ON o.order_id::text = s.reference_clean
)

SELECT *
FROM matched;
