import logging
import os
import sys

import dlt

from pipelines.esim.sources import esim_analytics
from utils import load_config


def run() -> None:
    if len(sys.argv) < 3 or not sys.argv[2]:
        raise ValueError("Please provide a group name as the second argument.")

    group_name = sys.argv[2]
    config = load_config(group_name, "esim")

    logging.info("Running esim Analytics Export pipeline for group: %s", group_name)

    suffix = os.getenv("PIPELINE_NAME_SUFFIX", "")
    pipeline = dlt.pipeline(
        pipeline_name=f"esim_analytics_{group_name}{suffix}",
        destination=dlt.destinations.clickhouse(destination_name="clickhouse_esim"),
        dataset_name="esim",
    )

    load_info = pipeline.run(
        esim_analytics(
            base_url=config["base_url"],
            api_key=config["api_key"],
        )
    )
    logging.info("Pipeline completed: %s", load_info)


if __name__ == "__main__":
    run()
