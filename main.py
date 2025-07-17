import logging
import sys

from pipelines import pg
from pipelines.facebook import facebook_ads_pipeline
from utils import setup_logging


def daily():
    logging.info("Running daily pipelines...")
    facebook_ads_pipeline.run()


if __name__ == "__main__":
    setup_logging()

    if len(sys.argv) > 1 and sys.argv[1] == "daily":
        # Run the pipeline immediately
        daily()
    else:
        logging.info("Starting pipelines...")
        pg.run()
