"""
App Store Connect ETL Pipeline
Extracts data from App Store Connect API and loads to ClickHouse.
"""
import logging
import os
import sys

import dlt

from app_store.helpers import get_app_store_client
from pipelines.app_store.sources import all_sources, get_days_back
from utils import get_for_group


def run():
    """
    Run the App Store Connect ETL pipeline.
    
    Usage:
        python main.py app_store <group_name>
        
    Environment Variables:
        APPSTORE_BACKFILL_DAYS: Number of days to backfill (default: 7)
        PIPELINE_NAME_SUFFIX: Optional suffix for pipeline name
    """
    if len(sys.argv) < 2 or not sys.argv[2]:
        raise ValueError("Please provide a group name as the second argument.")

    group_name = sys.argv[2]
    group, apps = get_for_group(group_name, "app_store")

    logging.info(f"Running App Store Connect pipeline for group: {group_name}")
    logging.info("Pulling apps: " + ", ".join([app.get("app_name", str(app)) for app in apps]))

    # Check for backfill mode
    days_back = get_days_back()
    backfill_days_env = os.getenv("APPSTORE_BACKFILL_DAYS")
    if backfill_days_env:
        logging.info(f"Backfill mode: pulling {days_back} days of data")

    # Create App Store Connect API client
    logging.info("Initializing App Store Connect API client...")
    
    # Read private key from file if path is provided
    private_key = group.get("private_key")
    if not private_key and group.get("private_key_path"):
        private_key_path = group["private_key_path"]
        logging.info(f"Reading private key from: {private_key_path}")
        with open(private_key_path, "r") as f:
            private_key = f.read()
    
    if not private_key:
        raise ValueError("Private key not found. Provide either 'private_key' or 'private_key_path' in config.")
    
    client = get_app_store_client(
        key_id=group["key_id"],
        issuer_id=group["issuer_id"],
        private_key=private_key
    )

    # Support custom pipeline name suffix for backfill runs
    pipeline_suffix = os.getenv("PIPELINE_NAME_SUFFIX", "")
    pipeline_name = f"app_store_{group_name}{pipeline_suffix}"

    pipe = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="clickhouse",
        dataset_name="app_store",
    )

    # Build sources for all apps
    sources = []
    for app_config in apps:
        app_id = app_config["app_id"]
        app_name = app_config.get("app_name", app_id)
        
        logging.info(f"Adding sources for app: {app_name} ({app_id})")
        
        for source_func in all_sources:
            # Some sources need days_back parameter
            if source_func.__name__ in ["customer_reviews", "builds"]:
                sources.append(
                    source_func(
                        client=client,
                        app_id=app_id,
                        app_name=app_name,
                        group_name=group_name,
                        days_back=days_back
                    )
                )
            else:
                sources.append(
                    source_func(
                        client=client,
                        app_id=app_id,
                        app_name=app_name,
                        group_name=group_name
                    )
                )

    logging.info(f"Running pipeline with {len(sources)} sources...")
    pipe.run(sources)
    logging.info("App Store Connect pipeline completed successfully.")
