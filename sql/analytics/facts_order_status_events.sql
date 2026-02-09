/*
fact_order_status_event.sql

Purpose:
Create an event fact table at grain = 1 row per status change event per order.

Why this exists:
public.orders is mutable (status is overwritten). Time-in-status metrics require status history.

Grain:
1 row = (order_id, status, status_ts, event_source)

Preferred Source (Design A):
public.order_status_audit (or equivalent)  -- append-only log of status transitions

Fallback Source (Design B):
Derive minimal events from public.orders timestamps (limited accuracy)

===========================================================
DESIGN A (PREFERRED): Audit / event log exists
===========================================================

Assumptions for Design A:
- The audit table contains one row per status transition
- Columns are *something like*:
    order_id, status, status_ts, ingested_at, source_file
*/

-- DESIGN A: Uncomment and adapt column names once ingestion exists
/*
SELECT
    a.order_id,
    a.status,
    a.status_ts,
    'audit_log'::text AS event_source,

    -- lineage
    a.ingested_at,
    a.source_file

FROM public.order_status_audit a;
*/


/*
===========================================================
DESIGN B (FALLBACK): No audit/event log exists
===========================================================

Reality:
If we only have public.orders snapshots, we cannot reconstruct every intermediate status.
But we CAN create a minimal "milestone events" table using reliable timestamps.

Assumptions for Design B:
- created_at exists for all orders
- delivered_at exists for delivered orders
- cancelled_at exists for cancelled orders
- (optional) accepted_at / picked_up_at may exist; if not, omit

This supports limited metrics:
- order cycle time (created -> delivered)
- cancellation timing (created -> cancelled)
But NOT:
- kitchen time (COOKING -> READY)
- driver wait time (ARRIVED -> READY)
unless those timestamps exist separately.
*/

SELECT
    -- stable event identifier (for dedupe + idempotent loads later)
    md5(
        concat_ws(
            '|',
            o.order_id::text,
            e.status::text,
            e.status_ts::text,
            'derived_from_orders'
        )
    ) AS event_id,

    o.order_id,
    e.status,
    e.status_ts::timestamptz AS status_ts,
    'derived_from_orders'::text AS event_source,

    -- lineage placeholders (fill in Phase 2 ingestion)
    NULL::timestamptz AS ingested_at,
    NULL::text        AS source_file

FROM public.orders o
CROSS JOIN LATERAL (
    VALUES
        ('PLACED'::text,    o.created_at),
        ('DELIVERED'::text, o.delivered_at),
        ('CANCELLED'::text, o.cancelled_at)

        /* Optional milestones if they exist in the source:
        ,('ACCEPTED'::text,  o.accepted_at)
        ,('PICKED_UP'::text, o.picked_up_at)
        */
) AS e(status, status_ts)
WHERE e.status_ts IS NOT NULL;


/*
Data Quality Notes:
- Design A is required for accurate time-in-status (kitchen time, driver wait time).
- Design B is only milestone-based and should be documented as limited.

Next Steps:
- During Phase 2 ingestion, prioritize capturing an order status audit stream.
- If the app emits events (Kafka, logs), ingest those as the event source.
*/
