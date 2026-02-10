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

        v.ingested_at,

        md5(
            concat_ws(
                '|',
                v.vendor_id::text,
                COALESCE(v.vendor_name, ''),
                COALESCE(v.cuisine_type, ''),
                COALESCE(v.commission_rate_decimal::text, ''),
                COALESCE(v.neighborhood_canonical, ''),
                COALESCE(v.neighborhood_raw, '')
            )
        ) AS source_row_hash
    FROM stg_vendor_onboarding_clean v
),

current_dim AS (
    SELECT
        d.vendor_id,
        d.source_row_hash AS current_row_hash,
        d.is_active
    FROM dim_vendor d
    WHERE d.is_current = TRUE
),

changes AS (
    SELECT
        i.*,
        cd.current_row_hash,
        cd.is_active AS current_is_active,
        CASE
            WHEN cd.vendor_id IS NULL THEN 'NEW'
            WHEN cd.current_row_hash <> i.source_row_hash THEN 'CHANGED'
            WHEN cd.is_active = FALSE THEN 'REACTIVATED'
            ELSE 'UNCHANGED'
        END AS change_type
    FROM incoming i
    LEFT JOIN current_dim cd
      ON cd.vendor_id = i.vendor_id
),

deactivations AS (
    SELECT
        cd.vendor_id,
        NULL::text    AS vendor_name,
        NULL::text    AS cuisine_type,
        NULL::numeric AS commission_rate_decimal,
        NULL::text    AS neighborhood_raw,
        NULL::text    AS neighborhood_canonical,

        -- set valid_from as ingestion time placeholder (will be actual in ETL)
        NOW()::timestamptz AS ingested_at,

        -- hash to represent "inactive"
        md5(concat_ws('|', cd.vendor_id::text, 'DEACTIVATED')) AS source_row_hash,

        cd.current_row_hash,
        cd.is_active AS current_is_active,
        'DEACTIVATED' AS change_type
    FROM current_dim cd
    LEFT JOIN incoming i
      ON i.vendor_id = cd.vendor_id
    WHERE i.vendor_id IS NULL
      AND cd.is_active = TRUE
)

SELECT
    vendor_id,
    vendor_name,
    cuisine_type,
    commission_rate_decimal,
    neighborhood_raw,
    neighborhood_canonical,

    ingested_at AS valid_from,
    NULL::timestamptz AS valid_to,
    TRUE AS is_current,

    CASE
      WHEN change_type = 'DEACTIVATED' THEN FALSE
      ELSE TRUE
    END AS is_active,

    ingested_at,
    source_row_hash,
    change_type
FROM changes
WHERE change_type IN ('NEW', 'CHANGED', 'REACTIVATED')

UNION ALL

SELECT
    vendor_id,
    vendor_name,
    cuisine_type,
    commission_rate_decimal,
    neighborhood_raw,
    neighborhood_canonical,

    ingested_at AS valid_from,
    NULL::timestamptz AS valid_to,
    TRUE AS is_current,
    FALSE AS is_active,

    ingested_at,
    source_row_hash,
    change_type
FROM deactivations;
