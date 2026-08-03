# eSIM order analytics 1.2 backfill

Deploy the **connector first while the backend still reports orders schema
`1.1`**. Run one successful hourly sync, then verify `SHOW CREATE TABLE
esim___orders` contains exactly the five new nullable columns with the expected
Decimal/bigint/bool types and that no existing column type or table engine
changed. Only then deploy backend schema `1.2`. This order prevents the old
connector from inferring the new money fields as floating-point columns.

The hourly eSIM pipeline then fills the five columns for new or subsequently
updated orders. It intentionally does not rewind existing dlt state.

To prepare historical values, run the **Stage eSIM Order Analytics 1.2
Backfill** workflow after the backend manifest reports orders schema `1.2`.
It writes only `id`, `subtotal_eur`, `profit_eur`, `item_count`,
`distinct_plan_count`, and `is_cart` to the separate
`esim___orders_analytics_fields_1_2_backfill` table. It never writes to
`esim___orders`.

The workflow automatically fails if staging contains duplicate IDs, unexpected
nulls, or missing/extra IDs compared with the logical existing orders table.

Before applying historical values:

1. Back up the orders table and record its row count plus a checksum excluding
   the five new columns.
2. Review the workflow's successful automated count, uniqueness, null, and ID
   coverage report.
3. Use a ClickHouse `Join`-engine helper plus `ALTER TABLE ... UPDATE` to update
   only the five new columns. Do not perform a dlt full merge of `orders`.
4. Wait for the mutation to finish in `system.mutations`.
5. Re-run the old-column checksum and require an exact match, then verify the
   new columns have no unexpected nulls.

The production table engine, ordering key, and exact `SHOW CREATE TABLE` output
must be reviewed before writing the mutation SQL. Record an old-column checksum
before and after a staging ClickHouse smoke test. The workflow deliberately
stops at the staging step.
