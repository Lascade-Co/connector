import logging
import os

import dlt
import sys

from dlt.sources.credentials import GcpOAuthCredentials

from google_ads import get_client
from pipelines.google.sources import all_sources, get_days_back
from utils import get_for_group


def run():
    if len(sys.argv) < 2 or not sys.argv[2]:
        raise ValueError("Please provide a group name as the second argument.")

    group_name = sys.argv[2]
    group, accounts = get_for_group(group_name, "google")

    logging.info(f"Running Google Ads pipeline for group: {group_name}")
    logging.info(f"Pulling accounts: {', '.join(accounts)}")

    # Check for backfill mode
    days_back = get_days_back()
    backfill_days_env = os.getenv("GOOGLE_BACKFILL_DAYS")
    if backfill_days_env:
        logging.info(f"Backfill mode: pulling {days_back} days of data")

    client = get_client(
        GcpOAuthCredentials(
            client_id=group["client_id"],
            client_secret=group["client_secret"],
            refresh_token=group["refresh_token"],
            project_id=group["project_id"]),
        group["dev_token"],
        group["email"],
        group["login_customer_id"]
    )

    # Support custom pipeline name suffix for backfill runs
    pipeline_suffix = os.getenv("PIPELINE_NAME_SUFFIX", "")
    pipeline_name = f"google_ads_{group_name}{pipeline_suffix}"

    pipe = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="clickhouse",
        dataset_name="google",
    )
    sources = []
    for customer_id in accounts:
        for source_func in all_sources:
            # campaign_budgets doesn't support date range, so we don't pass days_back to it
            if source_func.__name__ == "campaign_budgets":
                sources.append(source_func(client=client, customer_id=customer_id, group_name=group_name))
            else:
                sources.append(source_func(client=client, customer_id=customer_id, group_name=group_name, days_back=days_back))
    pipe.run(sources)
