import logging

import dlt

from constants import SELECTED_TABLES
from pipelines.pg.db_utils import get_last_record_info, get_pipeline, fetch_batched
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
    column_name, last_value = get_last_record_info(pg_table)

    if column_name and last_value:
        sql += f' WHERE "{column_name}" > %s'
        params += (last_value,)
    sql += f' LIMIT 2000000'
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
