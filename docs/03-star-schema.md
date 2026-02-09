# Star Schema (Draft v0)

This schema supports analytics for an on-demand delivery platform (customers, vendors, drivers, orders, payments, GPS).
It is designed to survive messy sources: mutable order rows, schema-drifting JSON, human-edited vendor sheets, duplicated GPS pings, and inconsistent settlement files.

---

## Design Principles

- **Stable grains**: every fact table has a clearly defined “one row per X” grain.
- **No double counting**: money and counts come from the correct fact table grain.
- **Event history matters**: mutable order rows are not enough for time-in-status metrics.
- **Late & duplicated data**: GPS and settlements are treated as append-only event streams with load metadata.

---

## Dimensions

### dim_date
- **PK**: date_id (YYYYMMDD)
- Attributes: date, day_of_week, week, month, quarter, year, is_weekend

### dim_time
- **PK**: time_id (HHMMSS)
- Attributes: hour, minute, second, part_of_day

### dim_customer
- **PK**: customer_id (from app DB)
- Attributes: created_at, first_order_at, acquisition_channel, acquisition_campaign (if available), is_test_user

### dim_vendor (SCD Type 2)
Tracks vendor attributes over time and protects analytics from Google Sheet edits/deletions.

- **PK**: vendor_sk (surrogate key)
- Natural key: vendor_id (from sheet/app)
- SCD fields: valid_from, valid_to, is_current
- Attributes: vendor_name, cuisine_type, commission_rate_decimal, neighborhood_raw, neighborhood_canonical, go_live_date, is_active

### dim_driver
- **PK**: driver_id
- Attributes: onboarding_date, vehicle_type (if available), home_area (if available), churn_date (derived), is_active

### dim_geo_area
Canonical neighborhood mapping.

- **PK**: geo_area_id
- Attributes: neighborhood_canonical, city, region
- Note: raw spellings map here via a staging mapping table.

---

## Facts

### fact_order
**Grain:** 1 row per order (order_id).  
This is the “order header” fact. It should not try to store every event; only stable order-level fields.

- **PK**: order_id
- **FKs**: customer_id, vendor_id (or vendor_sk via join), driver_id (nullable), created_date_id, created_time_id
- Core fields:
  - created_at
  - promised_eta_minutes (nullable)
  - order_value_app (subtotal in app)
  - delivery_fee
  - small_order_surcharge
  - discount_amount (nullable)
  - status_current (from mutable orders table)
  - cancelled_flag, cancelled_at (nullable)
  - delivered_at (nullable)

**Example metrics (safe):**
1. **Total Orders** = COUNT(DISTINCT order_id)
   - Safe because grain is already 1 row per order.
2. **Gross App Revenue** = SUM(order_value_app + delivery_fee + small_order_surcharge - discount_amount)
   - Safe because each order contributes once.
3. **Late Delivery Rate** = COUNTIF(delivered_at - created_at > 45 mins) / COUNTIF(delivered_at is not null)
   - Safe at order grain, but final “minutes” depends on reliable delivered_at (validated in staging).

---

### fact_order_status_event
**Grain:** 1 row per order status change event.  
Needed because the source orders table overwrites statuses.

- **PK**: (order_id, status, status_ts, event_source)  *(or a generated event_id)*
- **FKs**: order_id, status_date_id, status_time_id
- Fields:
  - status (PLACED, ACCEPTED, COOKING, READY, PICKED_UP, DELIVERED, CANCELLED, etc.)
  - status_ts
  - event_source (audit_log, derived, etc.)
  - ingested_at, source_file (lineage)

**Example metrics (safe):**
1. **Kitchen Time** = AVG(time between COOKING and READY)
   - Safe because we compute durations per order using event pairs, then aggregate.
2. **Driver Wait Time** = AVG(time between driver_arrived_at_vendor and READY)
   - Safe because computed at order-level first, then aggregated by vendor.
3. **Cancellation After Prep Rate** = cancellations where READY exists before CANCELLED / total orders
   - Safe because event history proves the sequence.

---

### fact_driver_assignment
**Grain:** 1 row per (order_id, driver_id, assignment_attempt).  
Used for dispatch analysis and “ghosting”.

- **PK**: assignment_id (generated)
- **FKs**: order_id, driver_id, assigned_date_id, assigned_time_id
- Fields:
  - assignment_attempt (1,2,3...)
  - assigned_ts
  - accepted_ts (nullable)
  - rejected_ts (nullable)
  - first_move_ts (nullable)
  - pickup_arrival_ts (nullable)
  - dropoff_ts (nullable)

**Example metrics (safe):**
1. **Acceptance Rate** = accepted / assigned
2. **Ghosting Rate** = accepted but no first_move_ts within X minutes
3. **Avg Time to Accept** = AVG(accepted_ts - assigned_ts)

---

### fact_gps_ping
**Grain:** 1 row per unique GPS ping after deduplication.  
Partitioned by **event date** (ping_ts) to handle late-arriving pings.

- **PK**: gps_ping_id (hash of driver_id + ping_ts + lat + lon)  *(generated)*
- **FKs**: driver_id, ping_date_id, ping_time_id
- Fields:
  - ping_ts
  - lat, lon
  - battery_level
  - ingestion_id / source_file
  - ingested_at
  - is_duplicate_flag (kept in staging for audit)

**Example metrics (safe):**
1. **Active Driver Minutes** = count of pings * 30 seconds (approx), per driver/day
2. **Distance Travelled (approx)** = sum of distance between consecutive pings per driver/day (computed in staging)
3. **Late Pings Volume** = count of pings where ingested_at date > ping_ts date

---

### fact_payment_settlement
**Grain:** 1 row per settlement line in the processor file.  
Some rows won’t match order_id; we keep them and reconcile separately.

- **PK**: settlement_line_id (generated from file + row number or hash)
- **FKs**: order_id (nullable), settlement_date_id
- Fields:
  - processor_reference_raw
  - order_id_clean (nullable after cleaning)
  - gross_amount
  - processor_fee
  - net_amount
  - currency
  - is_refund
  - original_reference (nullable)
  - file_name, ingested_at

**Example metrics (safe):**
1. **Net Settled Revenue** = SUM(net_amount) where is_refund = false
2. **Refund Amount** = ABS(SUM(net_amount)) where is_refund = true
3. **Settlement Match Rate** = count where order_id_clean is not null / total settlement rows

---

## Key Metric Rules (to avoid double counting)

- Order counts and “app price” revenue come from **fact_order**.
- Status durations come from **fact_order_status_event** (computed per order first).
- Dispatch/ghosting metrics come from **fact_driver_assignment**.
- Settled money comes from **fact_payment_settlement**, not fact_order.
- Any “Net Revenue per Order” metric must join:
  - app-side values (fact_order)
  - settlement values (fact_payment_settlement)
  - payouts/bonuses (to be added later as a separate fact if available)

---

## Open Questions (to resolve during EDA)

- What timestamps exist reliably in `public.orders` (created, updated, delivered)?
- Is there an accessible audit log for status transitions? If not, which transitions can be derived?
- Can GPS pings be linked to orders (trip/order id), or only to drivers?
- Do we have a driver payout/bonus dataset (separate from settlements)?
