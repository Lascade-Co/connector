"""
Loads the pipeline for Google Analytics V4.
Following the same pattern as Google Ads pipeline with group-based configuration.
"""
import logging
import os
import sys

import dlt
from dlt.sources.credentials import GcpOAuthCredentials

from pipelines.google_analytics.sources import all_sources, get_client, get_days_back
from utils import get_for_group


def run():
    """
    Main pipeline execution function.
    Loads GA4 data for a specific group with support for backfill mode.
    """
    if len(sys.argv) < 2 or not sys.argv[2]:
        raise ValueError("Please provide a group name as the second argument.")

    group_name = sys.argv[2]
    
    # Get group configuration and property IDs (reuse google group JSON)
    group, property_ids = get_for_group(group_name, "google_analytics")

    logging.info(f"Running GA4 pipeline for group: {group_name}")
    logging.info(f"Pulling properties: {', '.join(property_ids)}")

    # Check for backfill mode
    days_back = get_days_back()
    backfill_days_env = os.getenv("GA4_BACKFILL_DAYS")
    if backfill_days_env:
        logging.info(f"Backfill mode: pulling {days_back} days of data")

    # Create GA4 client with OAuth credentials (mirrors Google Ads pipeline)
    client = get_client(
        GcpOAuthCredentials(
            client_id=group["client_id"],
            client_secret=group["client_secret"],
            refresh_token=group["refresh_token"],
            project_id=group["project_id"],
        )
    )

    # Support custom pipeline name suffix for backfill runs
    pipeline_suffix = os.getenv("PIPELINE_NAME_SUFFIX", "")
    pipeline_name = f"ga4_{group_name}{pipeline_suffix}"

    pipe = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="clickhouse",
        dataset_name="google_analytics",
    )
    
    # Build sources for all properties
    sources = []
    for property_id in property_ids:
        for source_func in all_sources:
            sources.append(
                source_func(
                    client=client,
                    property_id=int(property_id),
                    group_name=group_name,
                    days_back=days_back,
                )
            )
    
    # Run the pipeline
    info = pipe.run(sources)
    logging.info(f"Pipeline completed: {info}")


if __name__ == "__main__":
    run()
