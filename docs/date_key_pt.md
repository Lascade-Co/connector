# PT-Aligned Day Key (`date_key_pt`)

## Overview

- **Goal**: Ensure reliable day-level joins between Google Analytics 4 (GA4) and Google Play datasets in ClickHouse and Metabase.
- **Problem**: GA4 dates are stored as `DateTime64(6, 'UTC')` (midnight UTC), while Google Play dates arrive as `String` `YYYY-MM-DD` representing the reporting day (PT-like). Joining by raw fields misaligns days.
- **Solution**: Add a materialized `Date` column named `date_key_pt` on all GA4 and Google Play tables:
  - GA4: `toDate(toTimeZone(date, 'America/Los_Angeles'))`
  - Play: `toDate(parseDateTimeBestEffort(date))`

This normalizes both sources to a single “business day” (PT) and enables fast, correct joins.

## Why `date_key_pt` is Needed

- **Timezone alignment**: Midnight UTC (GA4) is the previous calendar day in PT. Without normalization, day-level joins are off by one.
- **Performance**: Casting inside JOIN conditions prevents ClickHouse from leveraging data skipping and computed columns. A materialized key avoids per-query casting and improves performance.
- **Consistency**: Metabase models can expose a single Date field for joins and filtering, simplifying dashboards.

## Affected Tables

- GA4 (production):
  - `travel.google_analytics___ga4_events`
  - `travel.google_analytics___ga4_device_category`
  - `travel.google_analytics___ga4_session_traffic_sources`
  - `travel.google_analytics___ga4_traffic_sources`
  - `travel.google_analytics___ga4_user_engagement`
- Google Play (production):
  - `travel.google_play___google_play_installs`
  - `travel.google_play___google_play_ratings`
  - `travel.google_play___google_play_crashes`
  - `travel.google_play___google_play_store_performance`
- Repeat for staging tables if needed.

## Migration SQL

Run the following once (adjust database if different). After adding the columns, run `MATERIALIZE` to backfill existing data.

```sql
ALTER TABLE travel.google_analytics___ga4_events
  ADD COLUMN IF NOT EXISTS date_key_pt Date
  MATERIALIZED toDate(toTimeZone(date, 'America/Los_Angeles'));

ALTER TABLE travel.google_analytics___ga4_device_category
  ADD COLUMN IF NOT EXISTS date_key_pt Date
  MATERIALIZED toDate(toTimeZone(date, 'America/Los_Angeles'));

ALTER TABLE travel.google_analytics___ga4_session_traffic_sources
  ADD COLUMN IF NOT EXISTS date_key_pt Date
  MATERIALIZED toDate(toTimeZone(date, 'America/Los_Angeles'));

ALTER TABLE travel.google_analytics___ga4_traffic_sources
  ADD COLUMN IF NOT EXISTS date_key_pt Date
  MATERIALIZED toDate(toTimeZone(date, 'America/Los_Angeles'));

ALTER TABLE travel.google_analytics___ga4_user_engagement
  ADD COLUMN IF NOT EXISTS date_key_pt Date
  MATERIALIZED toDate(toTimeZone(date, 'America/Los_Angeles'));



ALTER TABLE travel.google_play___google_play_installs
  ADD COLUMN IF NOT EXISTS date_key_pt Date
  MATERIALIZED toDate(parseDateTimeBestEffort(date));

ALTER TABLE travel.google_play___google_play_ratings
  ADD COLUMN IF NOT EXISTS date_key_pt Date
  MATERIALIZED toDate(parseDateTimeBestEffort(date));

ALTER TABLE travel.google_play___google_play_crashes
  ADD COLUMN IF NOT EXISTS date_key_pt Date
  MATERIALIZED toDate(parseDateTimeBestEffort(date));

ALTER TABLE travel.google_play___google_play_store_performance
  ADD COLUMN IF NOT EXISTS date_key_pt Date
  MATERIALIZED toDate(parseDateTimeBestEffort(date));


ALTER TABLE travel.google_analytics___ga4_events
  ADD INDEX IF NOT EXISTS idx_date_key_pt date_key_pt TYPE minmax GRANULARITY 1;

ALTER TABLE travel.google_play___google_play_installs
  ADD INDEX IF NOT EXISTS idx_date_key_pt date_key_pt TYPE minmax GRANULARITY 1;


ALTER TABLE travel.google_analytics___ga4_events MATERIALIZE COLUMN date_key_pt;
ALTER TABLE travel.google_analytics___ga4_device_category MATERIALIZE COLUMN date_key_pt;
ALTER TABLE travel.google_analytics___ga4_session_traffic_sources MATERIALIZE COLUMN date_key_pt;
ALTER TABLE travel.google_analytics___ga4_traffic_sources MATERIALIZE COLUMN date_key_pt;
ALTER TABLE travel.google_analytics___ga4_user_engagement MATERIALIZE COLUMN date_key_pt;

ALTER TABLE travel.google_play___google_play_installs MATERIALIZE COLUMN date_key_pt;
ALTER TABLE travel.google_play___google_play_ratings MATERIALIZE COLUMN date_key_pt;
ALTER TABLE travel.google_play___google_play_crashes MATERIALIZE COLUMN date_key_pt;
ALTER TABLE travel.google_play___google_play_store_performance MATERIALIZE COLUMN date_key_pt;
```

## Verification Queries

- **Daily counts using `date_key_pt`**

```sql
SELECT date_key_pt AS d, count() AS g_rows
FROM travel.google_analytics___ga4_events
WHERE d BETWEEN '2025-10-01' AND '2025-10-12'
GROUP BY d
ORDER BY d DESC
LIMIT 5;

SELECT date_key_pt AS d, count() AS p_rows
FROM travel.google_play___google_play_installs
WHERE d BETWEEN '2025-10-01' AND '2025-10-12'
GROUP BY d
ORDER BY d DESC
LIMIT 5;
```

- **Join by `date_key_pt`**

```sql
SELECT g.date_key_pt AS d, count() AS rows
FROM travel.google_analytics___ga4_events AS g
JOIN travel.google_play___google_play_installs AS p
  ON g.date_key_pt = p.date_key_pt
WHERE g.date_key_pt BETWEEN '2025-10-01' AND '2025-10-12'
GROUP BY d
ORDER BY d DESC
LIMIT 5;
```

## Metabase Usage

- **Model fields**: Mark `date_key_pt` as `Date` on all GA4 and Google Play models.
- **Joins**: Define model joins using `date_key_pt`.
- **Report Timezone**: Consider setting Metabase Admin → Report Timezone to `America/Los_Angeles` to align UI filters and day boundaries with Google Play reporting.

## Query Patterns

- **Totals for a given PT date** (e.g., first_open vs store listing acquisitions):

```sql
WITH toDate('2025-10-06') AS d
SELECT
  d AS date_key_pt,
  (SELECT sumIf(event_count, event_name = 'first_open')
   FROM travel.google_analytics___ga4_events
   WHERE date_key_pt = d) AS first_open_events,
  (SELECT sumOrNull(store_listing_acquisitions)
   FROM travel.google_play___google_play_store_performance
   WHERE date_key_pt = d) AS store_listing_acquisitions;
```

- **Per-app (requires mapping `property_id` ↔ `package_name`)**:

```sql
WITH toDate('2025-10-06') AS d
SELECT
  p.date_key_pt,
  p.app_name,
  p.package_name,
  sumIf(g.event_count, g.event_name = 'first_open') AS first_open_events,
  sumOrNull(p.store_listing_acquisitions) AS store_listing_acquisitions
FROM travel.google_play___google_play_store_performance AS p
LEFT JOIN travel.app_mapping AS m
  ON m.package_name = p.package_name
LEFT JOIN travel.google_analytics___ga4_events AS g
  ON g.property_id = m.property_id AND g.date_key_pt = p.date_key_pt
WHERE p.date_key_pt = d
GROUP BY p.date_key_pt, p.app_name, p.package_name
ORDER BY p.app_name;
```

## Pipeline Follow-up (Optional ETL Enhancement)

The ClickHouse MATERIALIZED column ensures new inserts are populated automatically and historical data can be backfilled. Adding `date_key_pt` in the pipelines is therefore optional. Consider implementing it in ETL if you want the following benefits:

- **Portability**: If you may load to other destinations (that don’t support materialized columns), computing `date_key_pt` in code keeps schemas consistent.
- **Earlier availability**: Make `date_key_pt` available in staging/intermediate datasets before data reaches ClickHouse.
- **Per-group flexibility**: Allow different business timezones by group without needing DDL changes.
- **Simpler non-CH queries**: Downstream tools reading raw files or other stores can rely on the same day key.

Implementation notes if you opt in:

- GA4 (`pipelines/google_analytics/sources.py`): compute `date_key_pt` from `date` (UTC) by converting to PT and truncating to `Date`.
- Google Play (`pipelines/google_play/sources.py`): compute `date_key_pt` by parsing the `date` string to `Date`.
