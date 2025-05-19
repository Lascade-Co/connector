import logging
import sys

import dlt

from constants import SELECTED_TABLES
from pipelines.pg.db_utils import get_pipeline, get_last_created_at, fetch_batched
from utils import setup_logging


@dlt.resource(
    standalone=True,
    name=lambda args: args['pg_table'],
    write_disposition="merge",
    merge_key="id",
    primary_key="id",
)
def stream_table(pg_table: str):
    # Build source query with parameterized timestamp filter
    sql = f"SELECT * FROM {pg_table}"
    params = ()
    last_created_at = get_last_created_at(pg_table)

    if last_created_at:
        sql += " WHERE created_at > %s"
        params = (last_created_at,)

    yield from fetch_batched(sql, params)


def run() -> None:
    logging.info("Common tables pipeline started")
    pipe = get_pipeline("pg_to_click")

    streams = [stream_table(pg_table=tbl) for tbl in SELECTED_TABLES]
    pipe.run(streams)

    logging.info("Run finished")


if __name__ == "__main__":
    setup_logging()
    run()
