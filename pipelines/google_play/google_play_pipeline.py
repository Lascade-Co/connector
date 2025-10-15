"""
Google Play Console ETL Pipeline
Extracts data from Google Cloud Storage exports and loads to ClickHouse.
"""
import logging
import os
import sys

import dlt

from pipelines.google_play.sources import all_sources, get_months_back
from utils import get_for_group


def run():
    if len(sys.argv) < 2 or not sys.argv[2]:
        raise ValueError("Please provide a group name as the second argument.")

    group_name = sys.argv[2]
    group, apps = get_for_group(group_name, "google_play")

    logging.info(f"Running Google Play Console pipeline for group: {group_name}")
    logging.info("Pulling apps: " + ", ".join([app.get("package_name", str(app)) for app in apps]))

    # Check for backfill mode
    months_back = get_months_back()
    backfill_months_env = os.getenv("GOOGLE_PLAY_BACKFILL_MONTHS")
    if backfill_months_env:
        logging.info(f"Backfill mode: pulling {months_back} months of data")

    # Support custom pipeline name suffix for backfill runs
    pipeline_suffix = os.getenv("PIPELINE_NAME_SUFFIX", "")
    pipeline_name = f"google_play_{group_name}{pipeline_suffix}"

    pipe = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="clickhouse",
        dataset_name="google_play",
    )

    # Build sources for all apps
    sources = []
    for app_config in apps:
        package_name = app_config["package_name"]
        app_name = app_config.get("app_name", package_name)
        
        logging.info(f"Adding sources for app: {app_name} ({package_name})")
        
        for source_func in all_sources:
            sources.append(
                source_func(
                    credentials_path=group["credentials_path"],
                    bucket_name=group["bucket_name"],
                    package_name=package_name,
                    app_name=app_name,
                    months_back=months_back
                )
            )

    logging.info(f"Running pipeline with {len(sources)} sources...")
    pipe.run(sources)
    logging.info("Google Play Console pipeline completed successfully.")
