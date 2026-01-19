import logging
import time

import dlt

from pipelines.pg.db_utils import get_last_record_info, fetch_batched


@dlt.resource(
    standalone=True,
    name=lambda args: args['pg_table'],
    write_disposition="merge",
    merge_key="id",
    primary_key="id",
)
def _stream_table(pg_table: str, source: str, destination: str):
    # Build source query with parameterized timestamp filter
    sql = f"SELECT * FROM {pg_table}"
    params = ()
    column_name, last_value = get_last_record_info(pg_table, destination)

    if column_name and last_value:
        sql += f' WHERE "{column_name}" > %s'
        params += (last_value,)

    sql += f' ORDER BY "{column_name}" LIMIT 2000000'

    yield from fetch_batched(source, sql, params)


def run(selected_tables: list[str], pipe_line_name: str, dataset_name: str, source: str, destination: str) -> None:
    logging.info(f"{pipe_line_name} pipeline started")
    start = time.time()

    pipe = dlt.pipeline(
        pipeline_name=pipe_line_name,
        destination=destination,
        dataset_name=dataset_name,
    )

    streams = [_stream_table(pg_table=tbl, source=source, destination=destination) for tbl in selected_tables]
    pipe.run(streams)

    logging.info("Run finished in %.2f seconds", time.time() - start)
