import json, sys, dlt
import logging

from pipelines.facebook.sources import all_sources

def get_accounts(group_name):
    GROUPS = {g["name"]: g for g in json.load(open("fb_accounts.json", "r"))}
    group = GROUPS[group_name]

    # accounts → list of {"account_id": "...", "token": "..."}
    return [{"account_id": str(aid), "token": group["token"]} for aid in group["account_ids"]]


def run():
    if len(sys.argv) < 2 or not sys.argv[2]:
        raise ValueError("Please provide a group name as the second argument.")

    group_name = sys.argv[2]
    accounts = get_accounts(group_name)

    logging.info(f"Running Facebook Ads pipeline for group: {group_name}")
    logging.info(f"Pulling accounts: {', '.join(a['account_id'] for a in accounts)}")

    pipeline = dlt.pipeline(
        pipeline_name=f"fb_ads_{group_name}",  # <- each group has its *own* state dir
        destination="clickhouse",
        dataset_name="fb"
    )

    pipeline.run([source(accounts, group_name) for source in all_sources])

    logging.info("Facebook Ads pipeline completed successfully.")
