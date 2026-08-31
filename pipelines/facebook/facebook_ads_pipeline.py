import sys, dlt
import functools
import os
import logging
import re
import time

from pipelines.facebook.currency import (
    AedRateProvider,
    budget_currency_map,
    fx_rate_rows,
    insights_currency_map,
)
from pipelines.facebook.sources import (
    adsets_all,
    all_sources,
    campaigns_all,
    fx_daily_rates,
)
from pipelines.facebook.creative_status import reset_partial_resources
from pipelines.facebook.runner import (
    clickhouse_destination,
    raise_for_partial_resources,
    run_insights_in_windows,
    run_structural_resources,
)
from utils import enforce_local_facebook_group, get_for_group


PRODUCTION_DATASET = "fb"
_SMOKE_DATASET = re.compile(r"fb_smoke_[a-z0-9_]{1,32}")

# Only these current-state resources carry money (budgets and bids).
BUDGET_BEARING_SOURCES = (campaigns_all, adsets_all)


def resolve_dataset_name() -> str:
    """Allow an isolated smoke dataset, but never on a scheduled run.

    ``PIPELINE_NAME_SUFFIX`` only isolates dlt state; without this the dataset
    is always production ``fb``. Arbitrary names are rejected so a typo cannot
    silently strand a load in a new dataset.
    """

    requested = os.getenv("FB_DATASET_NAME")
    if not requested:
        return PRODUCTION_DATASET
    if requested == PRODUCTION_DATASET:
        return PRODUCTION_DATASET
    if os.getenv("GITHUB_EVENT_NAME") == "schedule":
        raise SystemExit(
            "FB_DATASET_NAME must not be set on scheduled runs; scheduled loads "
            f"always target {PRODUCTION_DATASET!r}."
        )
    if not _SMOKE_DATASET.fullmatch(requested):
        raise SystemExit(
            f"Invalid FB_DATASET_NAME={requested!r}; expected {PRODUCTION_DATASET!r} "
            "or a name matching fb_smoke_<lowercase alphanumeric/underscore>."
        )
    logging.warning("Loading into isolated dataset %s, not production", requested)
    return requested


def run():
    if len(sys.argv) < 3 or not sys.argv[2]:
        raise ValueError("Please provide a group name as the second argument.")

    group_name = sys.argv[2]
    enforce_local_facebook_group(group_name)
    group, accounts = get_for_group(group_name, "facebook")
    reset_partial_resources()

    logging.info(f"Running Facebook Ads pipeline for group: {group_name}")
    logging.info(f"Pulling accounts: {', '.join(accounts)}")

    suffix = os.getenv("PIPELINE_NAME_SUFFIX", "")
    pipeline = dlt.pipeline(
        pipeline_name=f"fb_ads_{group_name}{suffix}",  # <- each group has its *own* state dir
        destination=clickhouse_destination(),
        dataset_name=resolve_dataset_name(),
    )
    pipeline.sync_destination()

    delay_env = os.getenv("FB_ACCOUNT_DELAY_SECONDS", "0")
    try:
        delay_seconds = int(delay_env)
    except ValueError:
        logging.warning(
            "Invalid FB_ACCOUNT_DELAY_SECONDS=%r; defaulting to 0 seconds",
            delay_env,
        )
        delay_seconds = 0

    # One provider for the whole run: the ECB is fetched at most once, and an
    # all-AED group never reaches the network at all.
    rates = AedRateProvider()
    budgets_to_aed = budget_currency_map(rates, group["token"])
    insights_source, *structural_sources = all_sources
    insights_resource = functools.partial(
        insights_source, pre_flatten=insights_currency_map(rates)
    )
    structural_resources = [
        (
            functools.partial(source, row_transform=budgets_to_aed)
            if source in BUDGET_BEARING_SOURCES
            else source
        )
        for source in structural_sources
    ]

    for idx, account_id in enumerate(accounts):
        creds = [{"account_id": account_id, "token": group["token"]}]
        logging.info("Running Facebook Ads pipeline for account: %s", account_id)
        run_insights_in_windows(
            pipeline,
            insights_resource,
            creds,
            group_name,
            backfill_env_name="FB_BACKFILL_DAYS",
        )
        if os.getenv("FB_BACKFILL_DAYS"):
            logging.info("Insights backfill mode: skipping current-state resources")
        else:
            run_structural_resources(pipeline, structural_resources, creds, group_name)

        if idx < len(accounts) - 1 and delay_seconds > 0:
            logging.info(
                "Sleeping for %d seconds before next account to avoid rate limits",
                delay_seconds,
            )
            time.sleep(delay_seconds)

    _record_fx_rates(pipeline, rates)
    raise_for_partial_resources("Facebook")

    logging.info("Facebook Ads pipeline completed successfully.")


def _record_fx_rates(pipeline, rates: AedRateProvider) -> None:
    """Write the audit trail after the amounts it explains have committed.

    Every converted row already carries the rate it used, so a failure here
    loses only the audit table and the next run rewrites it.
    """

    rows = fx_rate_rows(rates.resolved_rates(), rates.retrieved_at)
    if not rows:
        logging.info("No currency conversion was needed; made no ECB requests")
        return
    provisional = sum(1 for row in rows if row["is_provisional"])
    logging.info(
        "Recording %d FX rates (%d provisional) from %d ECB request(s)",
        len(rows),
        provisional,
        rates.fetch_count,
    )
    pipeline.run(fx_daily_rates(rows))
