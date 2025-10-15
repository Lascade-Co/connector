import logging
import sys

from pipelines import pg
from pipelines.facebook import facebook_ads_pipeline
from pipelines.google import google_ads_pipeline
from pipelines.google_play import google_play_pipeline
from utils import setup_logging

if __name__ == "__main__":
    setup_logging()

    if len(sys.argv) == 1:
        logging.info("Starting pipelines...")
        pg.run()
    elif sys.argv[1] == "facebook":
        facebook_ads_pipeline.run()
    elif sys.argv[1] == "google":
        logging.info("doing nothing for now")
        google_ads_pipeline.run()
    elif sys.argv[1] == "google_play":
        google_play_pipeline.run()
