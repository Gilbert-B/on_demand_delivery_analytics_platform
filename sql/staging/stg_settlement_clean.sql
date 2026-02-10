/*
stg_settlement_clean.sql

Purpose:
Clean and standardize settlement CSV rows from the payment processor.

Key problems handled:
- Filename changes (tracked as metadata; not used for logic)
- Reference_ID sometimes null or prefixed with '#'
- Refund rows are negative and may not link to original transaction
- Amounts might be text with commas/currency symbols

Source (future):
raw.payment_settlement_csv

Output (future):
stg_settlement_clean
*/

WITH src AS (
    SELECT
        -- raw fields (names may differ per processor; adapt later)
        settlement_date,
        reference_id              AS reference_raw,
        transaction_id            AS transaction_id_raw,
        transaction_type          AS transaction_type_raw,
        currency                  AS currency_raw,
        amount_gross              AS gross_raw,
        processor_fee             AS fee_raw,
        amount_net                AS net_raw,

        -- file metadata / lineage
        source_file,
        source_row_number,
        ingested_at
    FROM raw.payment_settlement_csv
),

cleaned AS (
    SELECT
        -- normalize dates
        settlement_date::date AS settlement_date,

        -- normalize reference:
        -- 1) trim
        -- 2) remove leading '#'
        -- 3) treat empty string as NULL
        NULLIF(
            REGEXP_REPLACE(TRIM(reference_raw), '^#', ''),
            ''
        ) AS reference_clean,

        NULLIF(TRIM(transaction_id_raw), '') AS transaction_id,
        NULLIF(TRIM(transaction_type_raw), '') AS transaction_type,
        NULLIF(TRIM(currency_raw), '') AS currency,

        -- normalize numeric values:
        -- remove commas and any non-numeric symbols except '.' and '-'
        NULLIF(REGEXP_REPLACE(TRIM(gross_raw::text), '[^0-9\.\-]', '', 'g'), '')::numeric AS gross_amount,
        NULLIF(REGEXP_REPLACE(TRIM(fee_raw::text),   '[^0-9\.\-]', '', 'g'), '')::numeric AS processor_fee,
        NULLIF(REGEXP_REPLACE(TRIM(net_raw::text),   '[^0-9\.\-]', '', 'g'), '')::numeric AS net_amount,

        -- detect refunds (best-effort):
        -- if transaction_type says refund OR net is negative OR gross is negative
        CASE
            WHEN LOWER(COALESCE(transaction_type_raw, '')) LIKE '%refund%' THEN TRUE
            WHEN (NULLIF(REGEXP_REPLACE(TRIM(net_raw::text), '[^0-9\.\-]', '', 'g'), '')::numeric) < 0 THEN TRUE
            WHEN (NULLIF(REGEXP_REPLACE(TRIM(gross_raw::text), '[^0-9\.\-]', '', 'g'), '')::numeric) < 0 THEN TRUE
            ELSE FALSE
        END AS is_refund,

        -- stable settlement row id (idempotent loads)
        md5(
            concat_ws(
                '|',
                COALESCE(source_file, ''),
                COALESCE(source_row_number::text, ''),
                COALESCE(settlement_date::text, ''),
                COALESCE(TRIM(reference_raw), ''),
                COALESCE(TRIM(transaction_id_raw), '')
            )
        ) AS settlement_line_id,

        -- lineage
        source_file,
        source_row_number,
        ingested_at

    FROM src
)

SELECT
    settlement_line_id,
    settlement_date,
    reference_raw,
    reference_clean,
    transaction_id,
    transaction_type,
    currency,
    gross_amount,
    processor_fee,
    net_amount,
    is_refund,
    source_file,
    source_row_number,
    ingested_at
FROM cleaned;
