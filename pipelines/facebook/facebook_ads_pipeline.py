import json, os, sys, dlt
import logging

from pipelines.facebook.sources import all_sources


def get_accounts(group_name):
    GROUPS = {g["name"]: g for g in json.loads(os.getenv("fb_accounts.json"))}
    group = GROUPS[group_name]

    # accounts → list of {"account_id": "...", "token": "..."}
    return [{"account_id": aid, "token": group["token"]} for aid in group["account_ids"]]


def run():
    group_name = sys.argv[2]
    accounts = get_accounts(group_name)

    logging.info(f"Running Facebook Ads pipeline for group: {group_name}")
    logging.info(f"Pulling accounts: {', '.join(a['account_id'] for a in accounts)}")

    pipeline = dlt.pipeline(
        pipeline_name=f"fb_ads_{group_name}",  # <- each group has its *own* state dir
        destination="clickhouse",
        dataset_name="facebook_ads",  # <- *shared* final schema
        staging_dataset_name_layout="{dataset}__{pipeline}",
    )

    pipeline.run([source(accounts) for source in all_sources])

    logging.info("Facebook Ads pipeline completed successfully.")
