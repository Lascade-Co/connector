import logging
import sys
import time

import schedule

import pg_replication_pipeline

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s │ %(message)s", stream=sys.stdout
)

def safe_pg_pipeline():
    """Run the pg_replication_pipeline safely."""
    try:
        pg_replication_pipeline.run()
    except Exception as e:
        logging.exception(f"Unhandled pipeline error {e}", exc_info=True)


def main():
    schedule.every(1).hour.do(safe_pg_pipeline)

    while True:
        schedule.run_pending()
        time.sleep(60)
