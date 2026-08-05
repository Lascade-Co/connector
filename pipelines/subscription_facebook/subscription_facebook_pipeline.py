# Pipeline for pulling Facebook Ads data for subscription apps.
import sys, dlt
import os
import logging
import time

from pipelines.subscription_facebook.sources import (
    all_sources,
    get_partial_creative_accounts,
    reset_partial_creative_accounts,
)
from pipelines.facebook.runner import clickhouse_destination, run_insights_in_windows
from utils import enforce_local_facebook_group, get_for_group


def run():
    if len(sys.argv) < 3 or not sys.argv[2]:
        raise ValueError("Please provide a group name as the second argument.")

    group_name = sys.argv[2]
    enforce_local_facebook_group(group_name)
    group, accounts = get_for_group(group_name, "subscription_facebook")
    reset_partial_creative_accounts()

    logging.info(f"Running subscription Facebook Ads pipeline for group: {group_name}")
    logging.info(f"Pulling accounts: {', '.join(accounts)}")

    suffix = os.getenv("PIPELINE_NAME_SUFFIX", "")
    pipeline = dlt.pipeline(
        pipeline_name=f"subscription_fb_ads_{group_name}{suffix}",
        destination=clickhouse_destination("clickhouse_dashboard"),
        dataset_name="subscription_fb",
    )
    pipeline.sync_destination()

    delay_env = os.getenv("SUB_FB_ACCOUNT_DELAY_SECONDS", "0")
    try:
        delay_seconds = int(delay_env)
    except ValueError:
        logging.warning(
            "Invalid SUB_FB_ACCOUNT_DELAY_SECONDS=%r; defaulting to 0 seconds",
            delay_env,
        )
        delay_seconds = 0

    for idx, account_id in enumerate(accounts):
        creds = [{"account_id": account_id, "token": group["token"]}]
        logging.info(
            "Running subscription Facebook Ads pipeline for account: %s", account_id
        )
        run_insights_in_windows(
            pipeline,
            all_sources[0],
            creds,
            group_name,
            backfill_env_name="SUB_FB_BACKFILL_DAYS",
        )
        if os.getenv("SUB_FB_BACKFILL_DAYS"):
            logging.info("Insights backfill mode: skipping current-state resources")
        else:
            pipeline.run([source(creds, group_name) for source in all_sources[1:]])

        if idx < len(accounts) - 1 and delay_seconds > 0:
            logging.info(
                "Sleeping for %d seconds before next account to avoid rate limits",
                delay_seconds,
            )
            time.sleep(delay_seconds)

    partial_accounts = get_partial_creative_accounts()
    if partial_accounts:
        raise RuntimeError(
            "Subscription Facebook creatives were only partially loaded for account(s): "
            + ", ".join(partial_accounts)
        )

    logging.info("subscription Facebook Ads pipeline completed successfully.")
