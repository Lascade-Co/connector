# Pipeline for pulling Google Ads data for subscription apps.
# Auto-discovers child customer IDs from the configured MCC (login_customer_id).
import logging
import os
import sys

import dlt
from dlt.sources.credentials import GcpOAuthCredentials

from google_ads import get_client
from pipelines.subscription_google.sources import all_sources, get_days_back
from utils import load_config


def _discover_customer_ids(client, mcc_id: str, skip: set[str]) -> list[str]:
    svc = client.get_service("CustomerService")
    discovered = [rn.split("/")[-1] for rn in svc.list_accessible_customers().resource_names]
    # Always skip the MCC itself — not queryable for ad data.
    skip = set(skip) | {str(mcc_id)}
    return [cid for cid in discovered if cid not in skip]


def run():
    if len(sys.argv) < 3 or not sys.argv[2]:
        raise ValueError("Please provide a group name as the second argument.")

    group_name = sys.argv[2]
    group = load_config(group_name, "subscription_google")

    logging.info(f"Running subscription Google Ads pipeline for group: {group_name}")

    days_back = get_days_back()
    if os.getenv("SUB_GOOGLE_BACKFILL_DAYS"):
        logging.info(f"Backfill mode: pulling {days_back} days of data")

    client = get_client(
        GcpOAuthCredentials(
            client_id=group["client_id"],
            client_secret=group["client_secret"],
            refresh_token=group["refresh_token"],
            project_id=group["project_id"],
        ),
        group["dev_token"],
        group["email"],
        group["login_customer_id"],
    )

    skip = set(str(s) for s in group.get("skip_customer_ids", []))
    customer_ids = _discover_customer_ids(client, group["login_customer_id"], skip)
    logging.info(f"Discovered {len(customer_ids)} customer accounts under MCC {group['login_customer_id']}")
    logging.info(f"Pulling accounts: {', '.join(customer_ids)}")

    suffix = os.getenv("PIPELINE_NAME_SUFFIX", "")
    pipe = dlt.pipeline(
        pipeline_name=f"subscription_google_ads_{group_name}{suffix}",
        destination=dlt.destinations.clickhouse(destination_name="clickhouse_dashboard"),
        dataset_name="subscription_google",
    )

    sources = []
    for customer_id in customer_ids:
        for source_func in all_sources:
            if source_func.__name__ == "campaign_budgets":
                sources.append(source_func(client=client, customer_id=customer_id, group_name=group_name))
            else:
                sources.append(source_func(client=client, customer_id=customer_id, group_name=group_name, days_back=days_back))

    pipe.run(sources)
    logging.info("subscription Google Ads pipeline completed successfully.")
