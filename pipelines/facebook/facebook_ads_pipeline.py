import sys, dlt
import os
import logging
import time

from pipelines.facebook.sources import all_sources
from utils import get_for_group


def run():
    if len(sys.argv) < 2 or not sys.argv[2]:
        raise ValueError("Please provide a group name as the second argument.")

    group_name = sys.argv[2]
    group, accounts = get_for_group(group_name, "facebook")

    logging.info(f"Running Facebook Ads pipeline for group: {group_name}")
    logging.info(f"Pulling accounts: {', '.join(accounts)}")

    suffix = os.getenv("PIPELINE_NAME_SUFFIX", "")
    pipeline = dlt.pipeline(
        pipeline_name=f"fb_ads_{group_name}{suffix}",  # <- each group has its *own* state dir
        destination="clickhouse",
        dataset_name="fb"
    )

    delay_env = os.getenv("FB_ACCOUNT_DELAY_SECONDS", "600")
    try:
        delay_seconds = int(delay_env)
    except ValueError:
        logging.warning(
            "Invalid FB_ACCOUNT_DELAY_SECONDS=%r; defaulting to 600 seconds",
            delay_env,
        )
        delay_seconds = 600

    for idx, account_id in enumerate(accounts):
        creds = [{"account_id": account_id, "token": group["token"]}]
        logging.info("Running Facebook Ads pipeline for account: %s", account_id)
        # pipeline.run([all_sources[4](creds, group_name)]) # for insights only in local dev
        pipeline.run([source(creds, group_name) for source in all_sources])

        if idx < len(accounts) - 1 and delay_seconds > 0:
            logging.info(
                "Sleeping for %d seconds before next account to avoid rate limits",
                delay_seconds,
            )
            time.sleep(delay_seconds)

    logging.info("Facebook Ads pipeline completed successfully.")
