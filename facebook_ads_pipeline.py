import logging
import sys

import dlt

from facebook_ads import facebook_ads_source, facebook_insights_source

logging.basicConfig(level=logging.INFO, format="%(levelname)s │ %(message)s", stream=sys.stdout)


def run():
    logging.info("Starting Facebook Ads pipeline...")

    pipeline = dlt.pipeline(
        pipeline_name="fb_ads_to_clickhouse",
        destination="clickhouse",
    )

    # Metadata (full first run, then incremental merges)
    fb_ads = facebook_ads_source()

    # Metrics (backfill full history, then incremental)
    fb_insights = facebook_insights_source(initial_load_past_days=14)

    # Run both together
    info = pipeline.run([fb_ads, fb_insights])
    logging.info(f"Pipeline finished", info)


if __name__ == "__main__":
    run()
