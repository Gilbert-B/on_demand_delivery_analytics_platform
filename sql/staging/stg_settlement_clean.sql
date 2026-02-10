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
        settlement_date::date AS settlement_date,

        -- normalize reference: trim, remove leading '#', remove internal spaces
        NULLIF(
            REGEXP_REPLACE(
                REGEXP_REPLACE(UPPER(TRIM(reference_raw)), '^#', ''),
                '\s+',
                '',
                'g'
            ),
            ''
        ) AS reference_clean,

        NULLIF(TRIM(transaction_id_raw), '') AS transaction_id,
        NULLIF(TRIM(transaction_type_raw), '') AS transaction_type,
        NULLIF(TRIM(currency_raw), '') AS currency,

        -- strip to text first (safer + reusable)
        NULLIF(REGEXP_REPLACE(TRIM(gross_raw::text), '[^0-9\.\-]', '', 'g'), '') AS gross_amount_text,
        NULLIF(REGEXP_REPLACE(TRIM(fee_raw::text),   '[^0-9\.\-]', '', 'g'), '') AS processor_fee_text,
        NULLIF(REGEXP_REPLACE(TRIM(net_raw::text),   '[^0-9\.\-]', '', 'g'), '') AS net_amount_text,

        -- cast once
        NULLIF(REGEXP_REPLACE(TRIM(gross_raw::text), '[^0-9\.\-]', '', 'g'), '')::numeric AS gross_amount,
        NULLIF(REGEXP_REPLACE(TRIM(fee_raw::text),   '[^0-9\.\-]', '', 'g'), '')::numeric AS processor_fee,
        NULLIF(REGEXP_REPLACE(TRIM(net_raw::text),   '[^0-9\.\-]', '', 'g'), '')::numeric AS net_amount,

        CASE
            WHEN LOWER(COALESCE(transaction_type_raw, '')) LIKE '%refund%' THEN TRUE
            WHEN NULLIF(REGEXP_REPLACE(TRIM(net_raw::text), '[^0-9\.\-]', '', 'g'), '')::numeric < 0 THEN TRUE
            WHEN NULLIF(REGEXP_REPLACE(TRIM(gross_raw::text), '[^0-9\.\-]', '', 'g'), '')::numeric < 0 THEN TRUE
            ELSE FALSE
        END AS is_refund,

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

        source_file,
        source_row_number,
        ingested_at
    FROM src
)

