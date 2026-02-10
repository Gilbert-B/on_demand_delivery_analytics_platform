/*
stg_vendor_onboarding_scd2.sql

Purpose:
Convert cleaned vendor onboarding records into SCD Type 2 structure.

Inputs (future):
- stg_vendor_onboarding_clean
- existing dim_vendor (current version)

Output (future):
- stg_vendor_onboarding_scd2
*/

WITH incoming AS (
    SELECT
        v.vendor_id,
        v.vendor_name,
        v.cuisine_type,
        v.commission_rate_decimal,
        v.neighborhood_raw,
        v.neighborhood_canonical,

        -- lineage
        v.ingested_at,

        -- change detection fingerprint
        md5(
            concat_ws(
                '|',
                v.vendor_id::text,
                COALESCE(v.vendor_name, ''),
                COALESCE(v.cuisine_type, ''),
                COALESCE(v.commission_rate_decimal::text, ''),
                COALESCE(v.neighborhood_canonical, '')
            )
        ) AS source_row_hash

    FROM stg_vendor_onboarding_clean v
),

current_dim AS (
    -- current vendor versions (future: from dim_vendor)
    SELECT
        d.vendor_id,
        d.source_row_hash AS current_row_hash,
        d.valid_from,
        d.valid_to,
        d.is_current,
        d.is_active
    FROM dim_vendor d
    WHERE d.is_current = TRUE
),

changes AS (
    SELECT
        i.*,
        cd.current_row_hash,

        CASE
            WHEN cd.vendor_id IS NULL THEN 'NEW'
            WHEN cd.current_row_hash <> i.source_row_hash THEN 'CHANGED'
            ELSE 'UNCHANGED'
        END AS change_type

    FROM incoming i
    LEFT JOIN current_dim cd
        ON cd.vendor_id = i.vendor_id
)

SELECT
    vendor_id,
    vendor_name,
    cuisine_type,
    commission_rate_decimal,
    neighborhood_raw,
    neighborhood_canonical,

    -- SCD2 fields (rules applied in ETL step later)
    ingested_at AS valid_from,
    NULL::timestamptz AS valid_to,
    TRUE AS is_current,
    TRUE AS is_active,

    ingested_at,
    source_row_hash,
    change_type

FROM changes
WHERE change_type IN ('NEW', 'CHANGED');
