"""
App Store Connect ETL Pipeline
Extracts data from App Store Connect API and loads to ClickHouse.
"""
import logging
import os
import sys
import itertools

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
        APPSTORE_BACKFILL_DAYS: Number of days to backfill (triggers ONE_TIME_SNAPSHOT mode)
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
    is_backfill = os.getenv("APPSTORE_BACKFILL_DAYS") is not None
    if is_backfill:
        logging.info(f"BACKFILL MODE: Using ONE_TIME_SNAPSHOT for {days_back} days of historical data")
    else:
        logging.info(f"DAILY MODE: Using ONGOING reports for last {days_back} days")

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

    # Prefetch all accessible apps to resolve IDs reliably
    apps_by_bundle: dict[str, str] = {}
    apps_by_name: dict[str, str] = {}
    try:
        logging.info("Fetching list of accessible apps from App Store Connect…")
        for app in client.get_paginated("/v1/apps", params={"limit": 200}):
            app_id = app.get("id")
            attrs = app.get("attributes", {})
            b = attrs.get("bundleId")
            n = attrs.get("name")
            if app_id and b:
                apps_by_bundle[b] = app_id
            if app_id and n:
                apps_by_name[n] = app_id
        logging.info(f"Accessible apps: {len(apps_by_bundle)} (by bundleId)")
    except Exception as e:
        logging.error(f"Failed to list accessible apps: {e}")

    # Build sources for all apps
    sources = []
    for app_config in apps:
        app_name = app_config.get("app_name")
        bundle_id = app_config.get("bundle_id")

        asc_app_id = None

        try:
            # Prefer pre-fetched bundleId map
            if bundle_id and bundle_id in apps_by_bundle:
                asc_app_id = apps_by_bundle[bundle_id]
            # Fallback: try by name
            if not asc_app_id and app_name and app_name in apps_by_name:
                asc_app_id = apps_by_name[app_name]
            # Last resort: query by bundleId if not found in prefetch
            if not asc_app_id and bundle_id:
                logging.info(f"Resolving App Store Connect ID via bundle_id for {app_name} ({bundle_id})")
                resp = client.get("/v1/apps", {"filter[bundleId]": bundle_id, "limit": 1})
                items = resp.get("data", [])
                if items:
                    asc_app_id = items[0]["id"]
                    if not app_name:
                        app_name = items[0].get("attributes", {}).get("name", bundle_id)
        except Exception as e:
            logging.error(f"Failed to resolve App Store Connect ID for {app_name}: {e}")

        if not asc_app_id:
            sample_bundles = list(itertools.islice(apps_by_bundle.keys(), 10))
            logging.error(
                f"Could not resolve App Store Connect ID for {app_name} (bundle_id={bundle_id}). Skipping. "
                f"Sample accessible bundleIds: {sample_bundles}"
            )
            continue

        logging.info(f"Adding sources for app: {app_name} ({asc_app_id})")
        
        # Add analytics sources (engagement, commerce, usage)
        for source_func in all_sources:
            sources.append(
                source_func(
                    client=client,
                    app_id=asc_app_id,
                    app_name=app_name,
                    group_name=group_name,
                    is_backfill=is_backfill
                )
            )

    logging.info(f"Running pipeline with {len(sources)} sources...")
    if not sources:
        logging.error("No App Store sources to run. Verify bundle_id values in secrets and API key access to apps.")
        return
    pipe.run(sources)
    logging.info("App Store Connect pipeline completed successfully.")
