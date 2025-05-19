import logging
import sys

import dlt

from constants import SELECTED_TABLES
from pipelines.pg.db_utils import get_pipeline, get_pg_connection, get_ch_connection, CH


def get_destination_table_name(table_name: str) -> str:
    """Get the destination table name for the given source table name."""
    return f"{CH['database']}___{table_name}"


@dlt.resource(
    standalone=True,
    name=get_destination_table_name,
    write_disposition="merge",
    merge_key="id",
    primary_key="id",
)
def stream_table(table_name: str):
    ch_client = get_ch_connection()
    destination = get_destination_table_name(table_name)

    # Check if the destination table exists; if not, full initial load
    exists = ch_client.query(f"EXISTS TABLE {destination}").first_item
    if exists:
        # Get max(created_at) from ClickHouse (timezone preserved by driver)
        last_created_at = ch_client.query(f"SELECT MAX(created_at) FROM {destination}").first_item
    else:
        last_created_at = None

    # Build source query with parameterized timestamp filter
    sql = f"SELECT * FROM {table_name}"
    params = ()
    if last_created_at:
        sql += " WHERE created_at > %s"
        params = (last_created_at,)

    with get_pg_connection() as conn:
        # Batch fetching to limit memory footprint
        with conn.cursor() as cur:
            cur.itersize = 1000
            cur.execute(sql, params)
            row_count = 0
            while True:
                batch = cur.fetchmany(1000)
                if not batch:
                    break
                for row in batch:
                    yield row
                    row_count += 1

    # Log per-table row count
    logging.info(f"Streamed {row_count} rows from {table_name}")


def run() -> None:
    logging.info("Common tables pipeline started")
    pipe = get_pipeline("pg_to_click")

    streams = [stream_table(table_name=tbl) for tbl in SELECTED_TABLES]
    pipe.run(streams)

    logging.info("Run finished")


if __name__ == "__main__":
    try:
        run()
    except SystemExit as exc:
        logging.error("❌ %s", exc)
        sys.exit(1)
    except Exception as e:
        logging.exception(f"Unhandled pipeline error {e}", exc_info=True)
        sys.exit(2)
