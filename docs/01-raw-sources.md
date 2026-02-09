Here is the current state of our data ingestion. It is fragile, inconsistent, and requires significant cleaning before any reliable analysis can be done.

1. The public.orders Table (PostgreSQL Snapshot)

Origin: This is the core transactional database supporting the mobile app. We get a pg_dump of the production database.

Frequency: Daily full refresh at 02:00 UTC.

The Damage: The engineering team treats the order_details column as a "garbage bin." It is a massive, un-nested JSON blob containing item names, customizations, and prices, and the schema inside that JSON changes whenever the frontend team feels like it. Furthermore, there is no "historical" status tracking; when an order moves from COOKING to DELIVERED, the row is simply updated, meaning we have zero visibility into how long an order sat in the kitchen unless we scrape a separate, massive audit log.

2. Vendor Onboarding Sheet (Google Sheets)

Origin: The Sales and Account Management teams manually enter every new restaurant partner here before they go live on the app.

Frequency: Syncs every 15 minutes.

The Damage: This is human-entry chaos. The "Commission Rate" column contains values like 20%, 0.2, 20, and Twenty. Neighborhood names are spelled differently by every sales rep (e.g., "East Legon", "E. Legon", "East-Legon"). Worse, rows are sometimes hard-deleted by accident, causing active restaurants to suddenly disappear from our dimension tables, breaking joins downstream.

3. Driver GPS Pings (S3 Bucket / JSON Logs)

Origin: The driver app emits a GPS coordinate, timestamp, and battery level every 30 seconds. These are firehosed into an S3 bucket.

Frequency: Batched and loaded hourly.

The Damage: Because mobile networks are unstable, the app is aggressive about retrying failed uploads. This results in massive duplication—sometimes we get the same coordinate 50 times. Additionally, we suffer from "late arriving data," where a driver's phone comes back online and uploads GPS points from yesterday, messing up our daily distance calculations if we aren't careful with partition keys.

4. Payment Gateway Settlement Reports (CSV via FTP)

Origin: Weekly settlement files from our payment processor (e.g., Paystack or Stripe) showing what we were actually paid after fees.

Frequency: Files are dropped every Monday morning.

The Damage: The file naming convention changes randomly (e.g., settlement_v2_2024.csv vs 2024-settlement-final.csv). The Reference_ID column usually matches our order_id, but about 5% of the time it is null or formatted with a # prefix. Refund rows appear as negative values but often don't link back to the original transaction ID, making it nearly impossible to calculate true "Net Revenue" per order automatically.