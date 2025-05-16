import logging
import sys

from constants import SELECTED_TABLES
from pg_replication import replication_resource
from pg_replication.helpers import init_replication
from pipelines.pg.db_utils import get_pipeline


def run() -> None:
    SLOT = "analytics_slot"
    PUB = "analytics_pub"
    SCHEMA = "public"

    pipe = get_pipeline("pg_to_click")

    if pipe.first_run:
        logging.info("First run – taking initial snapshot")
        snapshot = init_replication(
            slot_name=SLOT,
            pub_name=PUB,
            schema_name=SCHEMA,
            table_names=SELECTED_TABLES,
            persist_snapshots=True,
            reset=True
        )

        logging.info("Snapshot complete, starting replication")

        pipe.run(snapshot)

    logging.info("Streaming logical changes …")
    pipe.run(replication_resource(SLOT, PUB))
    logging.info("Run finished")


if __name__ == "__main__":
    try:
        run()
    except SystemExit as exc:
        logging.error("❌  %s", exc)
        sys.exit(1)
    except Exception as e:
        logging.exception(f"Unhandled pipeline error {e}", exc_info=True)
        sys.exit(2)
