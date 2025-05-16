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
    logging.info("Starting pipelines...")
    main()
