/*
dim_vendor.sql

Purpose:
Vendor dimension with SCD Type 2 (history preserved).
Protects analytics from Google Sheet edits, overwrites, and deletions.

Grain:
1 row per vendor version (vendor_id + valid_from)

Keys:
- vendor_sk: surrogate primary key
- vendor_id: natural key from source systems

Source (future):
stg_vendor_onboarding_scd2
*/

SELECT
    -- surrogate key (generated during load)
    NULL::bigint AS vendor_sk,

    -- natural key
    v.vendor_id,

    -- descriptive attributes
    v.vendor_name,
    v.cuisine_type,

    -- standardized business fields
    v.commission_rate_decimal,
    v.neighborhood_raw,
    v.neighborhood_canonical,

    -- SCD Type 2 fields
    v.valid_from,
    v.valid_to,
    v.is_current,

    -- business state
    v.is_active,

    -- lineage / change detection
    v.ingested_at,
    v.source_row_hash

FROM stg_vendor_onboarding_scd2 v;
