import logging
import sys

from pipelines import pg

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s │ %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)


def safe_pg_pipeline():
    """Run the pg_replication_pipeline safely."""
    # noinspection PyBroadException
    try:
        pg.run()
    except Exception:
        logging.exception(f"Error in pg_replication_pipeline", exc_info=True)


def main():
    safe_pg_pipeline()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        # Run the pipeline immediately
        logging.info("Running pipeline immediately...")
        safe_pg_pipeline()
    else:
        logging.info("Starting pipelines...")
        main()
