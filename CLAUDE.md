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
python main.py facebook d1
python main.py google g1
python main.py google_analytics ga1
python main.py google_play gp1
python main.py app_store as1
python main.py pg dashboard

# eSIM pipelines (destination: clickhouse_esim → esim_db)
python main.py esim default
python main.py esim_facebook e1
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
FB_BACKFILL_DAYS=90 python main.py facebook d1
GA4_BACKFILL_DAYS=30 python main.py google_analytics ga1
ESIM_FB_BACKFILL_DAYS=90 python main.py esim_facebook e1
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
- `facebook_ads/` — Custom Facebook source with insights flattening, async job handling, timeout/retry logic, action metrics decomposition
- `google_analytics/` — Custom GA4 source with helper utilities
- `google_ads/` — Google Ads client integration

### Key Modules
- `utils.py` — Shared helpers: `get_for_group()` (loads group config with account_ids), `load_config()` (loads group config without account_ids assumption), date normalization, logging setup
- `facebook_ads/helpers.py` — Insights async job polling with exponential backoff, action metrics flattening
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

### Rate Limiting & Scaling
- Sequential batch processing for Facebook (3 workflow batches: daily-facebook-1/2/3)
- Configurable delays between accounts (`FB_ACCOUNT_DELAY_SECONDS`, default 600s)
- Facebook Insights uses async jobs with configurable timeout (`FB_INSIGHTS_JOB_TIMEOUT_SECONDS`) and retry (`FB_INSIGHTS_MAX_RETRIES`) with exponential backoff
- Max parallel: 1 per batch in GitHub Actions

## GitHub Actions Workflows
- `.github/workflows/_reusable-etl.yml` — Shared job template all pipelines use
- Daily schedules: Facebook 1:10 UTC, GA4 1:30 UTC, Google Ads 3:10 UTC, Google Play 5:10 UTC, App Store 6:10 UTC, eSIM Facebook 8:30 UTC
- Hourly: eSIM Analytics API every hour at :15
- `backfill.yml` — Manual dispatch workflow for historical data loads
- Secrets are base64-encoded in GitHub (DLT_SECRETS, FB_GROUPS, GOOGLE_GROUPS, etc.)

## Configuration
- `.dlt/secrets.toml` — dlt credentials (local dev)
- `.dlt/config.toml` — dlt runtime settings
- `secrets/*.json` — Per-platform account group definitions
- `.env` — Production connection strings (ClickHouse, PostgreSQL)
- Environment variables control backfill days, timeouts, delays, and retry counts

## eSIM Project Pipelines
The esim project uses a separate ClickHouse database (`esim_db`) via the `clickhouse_esim` dlt destination.

- **`pipelines/esim/`** — Analytics Export API pipeline. Manifest-driven: fetches dataset config (watermark fields, strategies, endpoints) from the backend's `/internal/analytics/exports/manifest/` at runtime. Column types are auto-detected by dlt from the API response data. Config in `secrets/esim.json`.
- **`pipelines/esim_facebook/`** — Facebook Ads pipeline for esim. Same pattern as `pipelines/facebook/` but targets `clickhouse_esim`. Config in `secrets/esim_facebook.json`. Uses `ESIM_FB_*` env vars.

## PostgreSQL Replication Pipeline
`pipelines/pg/` handles logical replication from remote PostgreSQL sources into ClickHouse, used for dashboard and travel datasets. This is distinct from the API-based pipelines.
