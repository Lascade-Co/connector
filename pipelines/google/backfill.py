"""Account-scoped, inclusive-date Google Ads backfills for GitHub Actions."""

import json
import logging
import os
import re
from pathlib import Path
from uuid import uuid4

import dlt
from dlt.sources.credentials import GcpOAuthCredentials

from google_ads import get_client
from pipelines.google.date_range import reporting_dates
from pipelines.google.sources import ads_metrics, campaign_metrics, conversion_actions
from utils import setup_logging

# Current-state resources use replace and must never join an account-only load.
METRIC_SOURCES = (ads_metrics, conversion_actions, campaign_metrics)


def normalize_account_id(value):
    value = str(value).strip()
    if re.fullmatch(r"[0-9]{3}-[0-9]{3}-[0-9]{4}", value):
        value = value.replace("-", "")
    if not re.fullmatch(r"[0-9]{10}", value):
        raise ValueError("Account ID must be a ten-digit Google Ads account ID or 'all'.")
    return value


def select_accounts(config, group_name, account_id):
    """Validate the entire selection before any API calls or destination writes."""
    group_name = group_name.strip()
    account_id = account_id.strip()
    target = None if account_id.lower() == "all" else normalize_account_id(account_id)
    groups = list(config) if group_name == "all" else [group_name]
    if not groups:
        raise ValueError("No Google Ads groups are configured.")
    selected = {}
    seen = set()
    for name in groups:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", name):
            raise ValueError("Invalid Google Ads group name.")
        if name not in config:
            raise ValueError(f"Group '{name}' is not configured. Available groups: {', '.join(config)}")
        accounts = config[name].get("account_ids")
        if not isinstance(accounts, list) or not accounts:
            raise ValueError(f"Group '{name}' must configure a nonempty account_ids list.")
        accounts = list(dict.fromkeys(normalize_account_id(value) for value in accounts))
        accounts = [value for value in accounts if target is None or value == target]
        if not accounts:
            continue
        if seen.intersection(accounts):
            raise ValueError("An account is configured in multiple selected groups; select one group.")
        seen.update(accounts)
        selected[name] = accounts
    if not selected:
        raise ValueError(f"Account {target} is not configured in the selected group(s).")
    return selected


def run():
    start, end = reporting_dates(
        start_date=os.environ.get("GOOGLE_BACKFILL_START_DATE", ""),
        end_date=os.environ.get("GOOGLE_BACKFILL_END_DATE", ""),
    )
    config = json.loads(Path("secrets/google.json").read_text())
    selected = select_accounts(
        config,
        os.environ.get("GOOGLE_BACKFILL_GROUP", "d1"),
        os.environ.get("GOOGLE_BACKFILL_ACCOUNT_ID", "all"),
    )
    logging.info("Configured Google Ads groups: %s", ", ".join(config))
    logging.info("Backfill range: %s through %s (inclusive)", start, end)
    logging.info("Selected accounts by group: %s", selected)
    suffix = os.environ.get("PIPELINE_NAME_SUFFIX", "")
    if not suffix:
        # Local runs must also avoid replaying an earlier backfill's load packages.
        suffix = f"_backfill_{uuid4().hex}"
    for name, accounts in selected.items():
        group = config[name]
        client = get_client(
            GcpOAuthCredentials(**{key: group[key] for key in (
                "client_id", "client_secret", "refresh_token", "project_id"
            )}),
            group["dev_token"], group["email"], group["login_customer_id"],
        )
        pipe = dlt.pipeline(
            pipeline_name=f"google_ads_{name}{suffix}",
            destination="clickhouse", dataset_name="google",
        )
        for account in accounts:
            resources = [source(
                client=client, customer_id=account, group_name=name,
                start_date=start.isoformat(), end_date=end.isoformat(),
            ) for source in METRIC_SOURCES]
            info = pipe.run(resources)
            if info is not None:
                info.raise_on_failed_jobs()
                logging.info("Account %s loaded: %s", account, info)
            else:
                logging.info("Account %s returned no data for the selected dates.", account)
            trace = pipe.last_trace
            normalized = trace.last_normalize_info if trace else None
            logging.info("Account %s row counts: %s", account, normalized.row_counts if normalized else {})
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a") as summary:
            summary.write(f"### Google Ads backfill completed\n\nDates: {start} through {end} (inclusive).\n\n")
            for name, accounts in selected.items():
                summary.write(f"- **{name}**: {', '.join(accounts)}\n")
            summary.write("\nRefreshed ad metrics, conversion actions, and campaign metrics using merges. Current-day data remains provisional.\n")


if __name__ == "__main__":
    setup_logging()
    run()
