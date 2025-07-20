import sys, dlt
import logging

from pipelines.facebook.sources import all_sources
from utils import get_for_group


def run():
    if len(sys.argv) < 2 or not sys.argv[2]:
        raise ValueError("Please provide a group name as the second argument.")

    group_name = sys.argv[2]
    group, accounts = get_for_group(group_name, "facebook")

    logging.info(f"Running Facebook Ads pipeline for group: {group_name}")
    logging.info(f"Pulling accounts: {', '.join(accounts)}")

    pipeline = dlt.pipeline(
        pipeline_name=f"fb_ads_{group_name}",  # <- each group has its *own* state dir
        destination="clickhouse",
        dataset_name="fb"
    )

    creds = [{"account_id": acc, "token": group["token"]} for acc in accounts]

    pipeline.run([source(creds, group_name) for source in all_sources])

    logging.info("Facebook Ads pipeline completed successfully.")
