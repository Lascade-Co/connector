import logging

import dlt
import sys

from dlt.sources.credentials import GcpOAuthCredentials

from google_ads import get_client
from pipelines.google.sources import all_sources
from utils import get_for_group


def run():
    if len(sys.argv) < 2 or not sys.argv[2]:
        raise ValueError("Please provide a group name as the second argument.")

    group_name = sys.argv[2]
    group, accounts = get_for_group(group_name, "google")

    logging.info(f"Running Google Ads pipeline for group: {group_name}")
    logging.info(f"Pulling accounts: {', '.join(accounts)}")

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

    pipe = dlt.pipeline(
        pipeline_name=f"google_ads_{group_name}",
        destination="clickhouse",
        dataset_name="google",
    )
    sources = []
    for customer_id in accounts:
        for source_func in all_sources:
            sources.append(source_func(client=client, customer_id=customer_id, group_name=group_name))
    pipe.run(sources)
