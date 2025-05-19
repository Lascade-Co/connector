import logging
import sys
import time

import schedule

from pipelines import pg
from utils import setup_logging


def safe_pg_pipeline():
    """Run the pg_replication_pipeline safely."""
    # noinspection PyBroadException
    try:
        pg.run()
    except Exception:
        logging.exception(f"Error in pg_replication_pipeline", exc_info=True)


def run():
    schedule.every(1).hours.do(safe_pg_pipeline)

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    setup_logging()

    if len(sys.argv) > 1 and sys.argv[1] == "schedule":
        # Run the pipeline immediately
        logging.info("Running pipeline immediately...")
        run()
    else:
        logging.info("Starting pipelines...")
        pg.run()
