/*
fact_payment_settlement.sql

Purpose:
Analytics fact table for payment processor settlements at grain = 1 row per settlement line.

Grain:
1 row = 1 settlement_line_id (processor settlement row)

Source:
stg_settlement_order_match

Notes:
- Some rows will not match orders (match_status != 'MATCHED'); we keep them for reconciliation.
- Refund rows may not link to the original purchase; we keep them and reconcile later.
*/

SELECT
    m.settlement_line_id,

    -- dates
    m.settlement_date,

    -- identifiers
    m.reference_raw,
    m.reference_clean,
    m.transaction_id,
    m.transaction_type,

    -- currency + amounts
    m.currency,
    m.gross_amount,
    m.processor_fee,
    m.net_amount,
    m.is_refund,

    -- order linkage (nullable)
    m.order_id,
    m.match_status,
    m.match_confidence,

    -- discrepancy helpers (nullable)
    m.app_total_amount,
    m.gross_discrepancy_amount,
    m.net_discrepancy_amount,

    -- lineage
    m.source_file,
    m.source_row_number,
    m.ingested_at

FROM stg_settlement_order_match m;
