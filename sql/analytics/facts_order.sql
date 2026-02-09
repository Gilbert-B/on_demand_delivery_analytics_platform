/* 
fact_order.sql

Purpose:
 create the analytics fact table at grain = 1 row per order

 Assumptions:
 - order_id uniquely identifies an order
 - orders table is mutable (status over written)
 - delivered_at may beNULL for cancelled/in_progress orders
 - monetary values are stored at the order header level 
 */

 SELECT 
        o.order_id,

        --dimensions
        o.customer_id,
        o.vendor_id,
        o.driver_id, 

        --timestamps
        o.created_at,
        o.delivered_at,
        o.cancelled_at,


        --order status
        o.status AS status_current,

        --monetary value
        o.subtotal_amount  AS order_value_app,
        o.delivery_fee,
        o.small_order_surcharge,
        o.discount_amount,

        --derived flags
        CASE
            WHEN o.cancelled_at IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_cancelled,

        CASE
            WHEN o.delivered_at IS NOT NULL THEN TRUE
            ELSE FALSE
        END AS is_delivered


        -- lineage / observability (placeholders until ingestion exists)
        /* These should be populated by the ingestion process in Phase 2 */
        NULL::timestamp AS ingested_at,
        NULL::date      AS source_snapshot_date

FROM public_orders o;


/*
Data Quality Notes:
- If driver_id is NULL, order was never assigned or assignment failed.
- delivered_at is unreliable for late delivery metrics without status events.
- monetary fields assume header-level consistency.

Risks:
- Mutable orders table hides time-in-status.
- Discounts may be applied at item level inside JSON (future work).

Next Steps:
- Validate columns during EDA once data is loaded.
- Replace status-based timing with event-based durations (fact_order_status_event).
*/