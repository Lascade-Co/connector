# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Multi-platform ETL pipeline system that extracts marketing/analytics data from 5 advertising platforms and loads into ClickHouse. Built on Python 3.12 with [dlt](https://dlthub.com/) as the core ETL framework.

**Data Sources:** Facebook Ads, Google Ads, Google Analytics 4, Google Play Console, App Store Connect, eSIM Analytics API
**Data Destinations:** ClickHouse `travel` (primary), ClickHouse `esim_db` (esim project), PostgreSQL (replication source)

## Commands

### Run a pipeline locally
```bash
# Pattern: python main.py <platform> <group>
python main.py facebook d1c
python main.py google g1
python main.py google_analytics ga1
python main.py google_play gp1
python main.py app_store as1
python main.py pg dashboard

# eSIM pipelines (destination: clickhouse_esim → esim_db)
python main.py esim default
python main.py esim_facebook d1c
```

### Install dependencies
```bash
pip install -r requirements.txt
```

### Local infrastructure
```bash
docker-compose up -d          # Start ClickHouse + PostgreSQL
docker-compose --profile metabase up -d  # Include Metabase
```

### Backfill mode
Set environment variables before running:
```bash
FB_BACKFILL_DAYS=90 python main.py facebook d1c
GA4_BACKFILL_DAYS=30 python main.py google_analytics ga1
ESIM_FB_BACKFILL_DAYS=90 python main.py esim_facebook d1c
```

## Architecture

### Entry Point & Dispatch
`main.py` → dispatches to `pipelines/<platform>/<platform>_pipeline.py` based on CLI args.

### Pipeline Structure
Each platform follows the same pattern:
1. **Pipeline runner** (`pipelines/<platform>/<platform>_pipeline.py`) — reads group config, creates dlt pipeline, executes sources
2. **Sources** (`pipelines/<platform>/sources.py`) — dlt resource definitions that transform API data into tables
3. **Group config** (`secrets/<platform>.json`) — JSON files defining account groups with credentials/tokens

### Custom dlt Sources (Modified from dlt verified sources)
- `facebook_ads/` — Custom Facebook source with incremental creative reconciliation, quota telemetry, bounded async Insights windows, and action metrics decomposition
- `google_analytics/` — Custom GA4 source with helper utilities
- `google_ads/` — Google Ads client integration

### Key Modules
- `utils.py` — Shared helpers: `get_for_group()` (loads group config with account_ids), `load_config()` (loads group config without account_ids assumption), date normalization, logging setup
- `facebook_ads/helpers.py` — Insights async job polling, explicitly API-bound account clients, and action metrics flattening
- `facebook_ads/settings.py` — Field definitions, insight breakdowns, action type selections

### Group-Based Account Management
Accounts are organized into named groups (d1, m4, d2, d1a, d1b, d1c, etc.). Each group:
- Has its own credentials in `secrets/<platform>.json`
- Maintains isolated dlt pipeline state (pipeline name includes group suffix)
- Runs independently in GitHub Actions

### Data Loading Patterns
- **Merge disposition** with primary keys for upsert behavior (most pipelines)
- **Replace disposition** for full-refresh tables
- dlt handles ClickHouse schema creation and evolution automatically
- Dataset/table naming uses configurable separators
- dlt merges emit `DELETE FROM`, which ClickHouse runs as a synchronous mutation. It must stay synchronous: the delete's `IN (SELECT ... FROM <temp>)` subquery is evaluated when the mutation runs, and dlt drops that temp table as soon as the statement returns
- ClickHouse 25.12 crashes vertical merges on tables carrying lightweight deletes (`Code: 295 ... ColumnGathererTransform (RECEIVED_EMPTY_DATA)`). The failed merge retries forever, and because the scheduler picks merges before mutations, every pending delete starves — the pipeline then hangs in `waitForMutation` until the socket dies. Any dlt merge table over 131k rows with 11+ non-PK columns is exposed; fix with `ALTER TABLE <t> MODIFY SETTING enable_vertical_merge_algorithm = 0`. Diagnose via `system.part_log` where `error = 295`

### Rate Limiting & Scaling
- Facebook A/B/C matrices mix independent `d1`/`m4`/`d2` families, spacing each family's suffix shards by roughly one hour; account delays default to zero
- Insights use separately committed async date windows, 100-row result pages, and split incompatible unique metrics; quota failures surface without stacked retries
- Creatives are incremental Monday-Saturday and fully reconciled Sunday or with manual workflow mode `full`
- Partial current-state loads commit valid merges; later reconciliation repairs only the missing subset
- Daily Insights expose trial and subscription counts and values as four scalar columns derived from Meta action arrays
- The `insights__actions`, `insights__action_values`, and `insights__website_ctr` nested tables are read directly by the Metabase model `fb_insights_base_data` (id 587). Do not stop populating, truncate, or drop them. The flattened scalar columns are not a substitute: `_expand_action_list` keeps the first entry per `action_type` while the model sums them, and Meta returns repeat entries often enough that ~4% of rows disagree
- Facebook historical backfills load Insights only and serialize multi-group matrices; current-state resources use the daily/manual Facebook workflows

### Facebook currency normalization (travel `fb` dataset only)
Not every Facebook account bills in AED, but Metabase compares them as if they did. `pipelines/facebook/currency.py` restates every monetary value in AED **in place**, so `spend`, `cpc`, `cpm`, `cpp` and the nested `value` columns stay AED and **no Metabase consumer needs a currency change**. Verified consumers reading `travel.fb___insights` directly: model 587 `fb_insights_base_data`, and questions 601, 603, 948, 367.

- **The source currency comes from Meta, not from config.** `pipelines/facebook/raw_sources.py` adds `account_currency` to the requested Insights fields, so each row states its own currency. There is deliberately no per-account currency map to keep in sync. `secrets/facebook.json` needs no currency key.
- **Conversion runs pre-flatten.** `facebook_insights_source(pre_flatten=...)` applies the map *before* `flatten_facebook_insights`, so every scalar derived from a nested array (`action_values_*`, `cpa_*`, `trial_start_value`, `subscription_value`, `cost_per_result`) inherits AED without being enumerated. Do not move this after flattening.
- **Two destination type families, and each is written back as it already is.** `spend`/`cpc`/`cpm`/`cpp` and every nested `value` are `Nullable(String)` — write `str(Decimal)`. The flattened scalars (`action_values_*`, `cpa_*`, `cost_per_result`, `trial_start_value`, `subscription_value`) are `Nullable(Float64)` and Metabase renders them as numbers — they must stay Float64. Arithmetic is `Decimal` throughout, quantized at six places.
- **`INSIGHT_FIELDS_TYPES` hints are dead for existing columns.** It hints `spend` as `decimal` while the live column is `String`; dlt never retypes an existing column. Never assume adding a hint took effect — check the live type.
- **The attribution-window columns are money too.** `insights__action_values` and `insights__cost_per_action_type` carry `_1d_click`/`_7d_click`/`_28d_click`/`_1d_view`/`_7d_view`/`_28d_view` alongside `value`, populated in 93–99% of rows. All are converted (matched by shape, so a new window Meta adds is covered). Converting only `value` would leave one row holding two currencies.
- **Not converted, deliberately:** `purchase_roas` (value and spend share a currency, so the ratio is invariant), `ctr`/`unique_ctr`/`frequency`/`website_ctr`, and all counts (`impressions`, `clicks`, `reach`, `actions_*`, `trial_starts`, `subscriptions`).
- **The rate travels on the row.** Every row carries `source_currency`, `reporting_currency`, `fx_rate_to_aed`, `fx_requested_date`, `fx_effective_date`, `fx_rate_status` and `spend_source`. Nested entries keep `value_source`. Because the rate used is denormalized, `fb___fx_daily_rates` is an **audit record, not a dependency** — Insights never reads it back, so there is no cross-table atomicity problem to solve. If a rate cannot be resolved the map raises, the window's `pipeline.run` fails, nothing commits and the checkpoint does not advance.
- **Rates come from the ECB, cross-multiplied through the USD/AED peg:** `aed_per_gbp = (usd_per_eur / gbp_per_eur) * 3.6725`, both legs joined on `TIME_PERIOD` because the API groups rows by series, not by date. `fx_rate_status` is `exact` when the day was published, `carried` when a later observation proves the ECB moved past a non-publication day (final), and `provisional` when it may still be restated — the 7-day attribution reload settles those. A rate more than 4 days old warns; more than 7 days old fails the window closed.
- **An AED account short-circuits before any network call** and its stored strings stay byte-identical, so "no ECB request occurred" is a meaningful canary check. This is why the identity path skips the arithmetic instead of re-quantizing.
- **This is a reporting rate, not the ledger cost.** Meta bills GBP accounts in GBP; the real AED cost includes the card/bank FX spread (typically 1–3%), so GBP spend will not tie to finance's books.
- **Budgets are minor units.** `daily_budget`/`lifetime_budget` (text) and `bid_amount` (Int64) are converted with the same rate — AED and GBP are both two-decimal currencies, so no unit adjustment is needed — and rounded back to whole minor units. The account currency here comes from what Insights already reported; Meta is asked only for an account Insights returned no rows for.
- **Scoped to travel.** eSIM and subscription Facebook pass no `pre_flatten` and no `row_transform`, so their behavior and columns are unchanged. `DEFAULT_INSIGHT_FIELDS` was deliberately not modified.
- Local Facebook, eSIM Facebook, and subscription Facebook execution is restricted to exact group `d1c`; all other groups are GitHub Actions-only
- Max parallel: 1 per batch in GitHub Actions
- **Scheduled runs alert on failure.** `_reusable-etl.yml` ends with an `if: failure()` step that posts the pipeline, group, and run URL to `SLACK_WEBHOOK_URL`. It is **inert until that repo secret is set** (logs a skip and exits 0) and ends in `|| true`, so a Slack outage can never mask the real ETL failure. Before this existed nothing announced a broken run — the hourly eSIM sync failed silently for as long as it took someone to open the Actions tab.
- **`tests.yml` runs `python -m unittest discover -s tests -t .`** on push to `main`, PRs, and manual dispatch. The repo previously had no test workflow at all, so `tests/` only ran by hand. Test modules import `dlt`/`facebook_business` at module scope, so the job installs the full `requirements.txt` — a partial install surfaces as collection errors, not failures.

## GitHub Actions Workflows
- `.github/workflows/_reusable-etl.yml` — Shared job template all pipelines use
- Daily schedules (UTC): Facebook batches A/B/C 0:00/1:00/2:00 (5:30/6:30/7:30 IST), Subscription Google 2:50 (8:20 IST), Google Ads 3:00 (8:30 IST), Subscription Facebook 3:00 (8:30 IST), GA4 4:30, Google Play 5:10, App Store 6:10, eSIM Facebook 8:30
- Hourly: eSIM Analytics API every hour at :15
- `backfill.yml` — Manual dispatch workflow for historical data loads
- Secrets are base64-encoded in GitHub (DLT_SECRETS, FB_GROUPS, GOOGLE_GROUPS, etc.)

## Configuration
- `.dlt/secrets.toml` — dlt credentials (local dev)
- `.dlt/config.toml` — dlt runtime settings
- `secrets/*.json` — Per-platform account group definitions
- `.env` — Production connection strings (ClickHouse, PostgreSQL)
- Environment variables control backfill days, timeouts, delays, and retry counts
- **`FB_DATASET_NAME`** overrides the travel Facebook dataset for isolated smoke runs. `PIPELINE_NAME_SUFFIX` only isolates dlt *state* — without this the dataset is always `fb`. Only `fb` or `fb_smoke_<lowercase alphanumeric/underscore>` is accepted, and any override is refused when `GITHUB_EVENT_NAME=schedule`, so a scheduled run can never land outside production. Note dlt also mirrors the name for its merge staging dataset (`fb_staging___*`), which is what makes the isolation complete.

## eSIM Project Pipelines
The esim project uses a separate ClickHouse database (`esim_db`) via the `clickhouse_esim` dlt destination.

- **`pipelines/esim/`** — Analytics Export API pipeline. Manifest-driven: fetches dataset config (watermark fields, strategies, endpoints) from the backend's `/internal/analytics/exports/manifest/` at runtime. Most column types are auto-detected; reviewed additive fields use explicit hints where stable inference matters. Config in `secrets/esim.json`.
- Order analytics remain one row per order.
- **Schema-version gating is on MAJOR, not exact match.** `SUPPORTED_SCHEMA_VERSIONS` in `pipelines/esim/constants.py` records the versions actually *reviewed* here; `manifest._resolve_schema_version` then accepts any **MINOR** of a reviewed **MAJOR** with a loud `SCHEMA DRIFT` warning, and hard-fails an unreviewed **MAJOR** (or a malformed version). Datasets absent from the dict are ungated. Reviewed today: users through `1.2`, sessions through `1.2`, and orders through `1.5`.
  - Rationale: the backend bumps MINOR for additive columns only (1.2, 1.3, 1.4 all were) and dlt never retypes an existing column, so only *new* columns are at stake. Exact matching meant every additive backend field hard-stopped the **whole** hourly run (all datasets, not just orders) until someone edited the set — which is exactly what happened when the backend shipped orders `1.4`. A MAJOR bump can change or drop existing columns, so it stays gated.
  - The highest-risk MINOR addition is a **new money column**: the backend's `to_numeric` sends `Decimal` as a JSON float, so an unhinted one infers as `double`. This is why money fields are pinned in `DATASET_COLUMN_HINTS`; attribution and funnel columns are also pinned where nullability or an initially empty dataset could make inference unstable. On a `SCHEMA DRIFT` warning, check the new fields and add any necessary hints before recording the version as reviewed.
  - Order version history: `1.2` added `subtotal_eur`/`profit_eur`/`item_count`/`distinct_plan_count`/`is_cart`; `1.3` added `payment_method`/`stripe_account_slug`/`stripe_account_acct_id`; `1.4` added `support_resolution` and made wallet-compensated orders export `status='refunded'` — nothing reads refunds today, but if that changes, split real Stripe reversals (`support_resolution=''`) from wallet credits (`'compensated'`).
  - Sessions `1.2` adds the four funnel booleans (`plans_viewed`, `plan_added`, `checkout_viewed`, `purchased`) and their nullable `*_at` timestamps. Keep explicit bool/timestamp hints for these fields so empty early-stage data cannot cause unstable inference.
  - Column hints are selected by the manifest's dataset version through `DATASET_COLUMN_HINT_MIN_VERSIONS`; keep that map updated so a connector-first deploy never declares future non-null columns against an older backend export.
  - The backend mirrors this contract in `analytics_export/views/manifest.py` and pins the version in `analytics_export/tests/test_orders.py`, so a bump there fails a test that names this file.
- **`pipelines/esim_facebook/`** — Facebook Ads pipeline for esim. Same pattern as `pipelines/facebook/` but targets `clickhouse_esim`. Config in `secrets/esim_facebook.json`. Uses `ESIM_FB_*` env vars and the shared creative quota guard.

## PostgreSQL Replication Pipeline
`pipelines/pg/` handles logical replication from remote PostgreSQL sources into ClickHouse, used for dashboard and travel datasets. This is distinct from the API-based pipelines.
