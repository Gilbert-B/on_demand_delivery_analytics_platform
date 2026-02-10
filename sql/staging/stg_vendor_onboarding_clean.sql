/*
stg_vendor_onboarding_clean.sql

Purpose:
Clean and standardize vendor onboarding data from Google Sheets.

Key problems handled:
- commission_rate formats: 20%, 0.2, 20, "Twenty"
- neighborhood spelling variants: "East Legon", "E. Legon", "East-Legon"
- hard deletes in sheets (handled later in SCD2 step by tracking history)

Source (future):
raw.vendor_onboarding_sheet

Output (future):
stg_vendor_onboarding_clean
*/

WITH src AS (
    SELECT
        vendor_id,
        vendor_name,
        cuisine_type,
        commission_rate:: text AS commission_rate_raw,
        neighborhood    AS neighborhood_raw,
        updated_at      AS source_updated_at,

        -- lineage
        ingested_at,
        source_row_number,
        source_file
    FROM raw.vendor_onboarding_sheet
),

standardized AS (
    SELECT
        vendor_id,
        NULLIF(TRIM(vendor_name), '') AS vendor_name,
        NULLIF(TRIM(cuisine_type), '') AS cuisine_type,

        -- commission standardization (decimal form: 0.20 for 20%)
        CASE
            WHEN commission_rate_raw IS NULL OR TRIM(commission_rate_raw) = '' THEN NULL

            -- "20%" -> 0.20
            WHEN TRIM(commission_rate_raw) LIKE '%\%%' ESCAPE '\' THEN
                (NULLIF(REPLACE(TRIM(commission_rate_raw), '%', ''), '')::numeric) / 100

            -- "0.2" -> 0.2
            WHEN TRIM(commission_rate_raw) ~ '^[0-9]+(\.[0-9]+)?$' THEN
                CASE
                    WHEN TRIM(commission_rate_raw)::numeric > 1 THEN TRIM(commission_rate_raw)::numeric / 100
                    ELSE TRIM(commission_rate_raw)::numeric
                END

            -- "Twenty" (fallback: NULL; handled with mapping later if you want)
            ELSE NULL
        END AS commission_rate_decimal,

        -- keep raw for audit
        neighborhood_raw,

        -- basic neighborhood canonicalization (final mapping table comes next)
        LOWER(
            REGEXP_REPLACE(COALESCE(TRIM(neighborhood_raw), ''),'[^a-zA-Z0-9 ]', '', 'g')
        ) AS neighborhood_normalized,

        source_updated_at,

        -- lineage
        ingested_at,
        source_row_number,
        source_file

    FROM src
)

SELECT
    vendor_id,
    vendor_name,
    cuisine_type,
    commission_rate_decimal,
    neighborhood_raw,

    -- placeholder: will be mapped to canonical names via mapping table later
    neighborhood_normalized AS neighborhood_canonical,

    source_updated_at,
    ingested_at,
    source_row_number,
    source_file

FROM standardized;
