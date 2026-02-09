/* 
fact_order.sql

Purpose:
 create the analytics fact table at grain = 1 row per order

 Assumptions:
 - order_id uniquely identifies an order
 - orders table is mutable (status over written)
 - delivered_as may beNULL for cancelled/in_progress orders
 - monetary values are stored at the order header level 
 */

 SELECT 
        o.order_id,

        --dimensions
        o.customer_id,
        o.vendor_id,
        o.driver_id, 

        --timestamps
        o.vreated_at,
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
            ESLE FALSE
        END AS is_delivered

FROM public_orders o;


/*
Data Quality Notes:
- If driver_id is NULL, order was never assigend or assignement failed
- delivered_at is unreliable for late delivery metrics without status events 
- monetary fields assume header level cons9stency 

Risks:
- Mutable Orders table hides time_in_status
_ DIscounts may be applied at item level inside JSON 

Next Steps :

- Validate columns during EDA once data ius loaded 
- Replace stust logic with event_based duratiopns
*/