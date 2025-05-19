import logging
import sys

import dlt

from constants import SELECTED_TABLES
from pipelines.pg.db_utils import get_pipeline, get_pg_connection, get_ch_connection, CH


def get_destination_table_name(table: str) -> str:
    """Get the destination table name for the given source table name."""
    return f"{CH['database']}___{table}"


@dlt.resource(
    standalone=True,
    name=lambda args: args['pg_table'],
    write_disposition="merge",
    merge_key="id",
    primary_key="id",
)
def stream_table(pg_table: str):
    ch_client = get_ch_connection()
    destination = get_destination_table_name(pg_table)

    # Check if the destination table exists; if not, full initial load
    exists_count = ch_client.query(
        "SELECT count() as cnt FROM system.tables WHERE database = %s AND name = %s",
        (CH['database'], destination)
    ).first_item["cnt"]

    last_created_at = None

    if exists_count > 0:
        # Get max(created_at) from ClickHouse (timezone preserved by driver)
        try:
            last_created_at = ch_client.query(
                f"SELECT MAX(created_at) as last FROM `{CH['database']}`.`{destination}`"
            ).first_item["last"]
        except Exception:
            pass

    # Build source query with parameterized timestamp filter
    sql = f"SELECT * FROM {pg_table}"
    params = ()
    if last_created_at:
        sql += " WHERE created_at > %s"
        params = (last_created_at,)

    with get_pg_connection() as conn:
        # Batch fetching to limit memory footprint
        with conn.cursor() as cur:
            cur.itersize = 2000
            cur.execute(sql, params)
            while True:
                batch = cur.fetchmany(1000)
                if not batch:
                    break
                for row in batch:
                    yield row


def run() -> None:
    logging.info("Common tables pipeline started")
    pipe = get_pipeline("pg_to_click")

    streams = [stream_table(pg_table=tbl) for tbl in SELECTED_TABLES]
    pipe.run(streams)

    logging.info("Run finished")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s │ %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    try:
        run()
    except SystemExit as exc:
        logging.error("❌ %s", exc)
        sys.exit(1)
    except Exception as e:
        logging.exception(f"Unhandled pipeline error {e}", exc_info=True)
        sys.exit(2)
