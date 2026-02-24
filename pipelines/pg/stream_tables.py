import logging
import time

import dlt

from pipelines.pg.db_utils import get_last_record_info, fetch_batched

TableMapping = dict[str, tuple[str, dict | None]]

@dlt.resource(
    standalone=True,
    name=lambda args: args['pg_table'],
    write_disposition="merge",
    merge_key="id",
    primary_key="id",
)
def _stream_table(pg_table: str, source: str, destination: str):
    # Build a source query with a parameterized timestamp filter
    sql = f"SELECT * FROM {pg_table}"
    params = ()
    column_name, last_value = get_last_record_info(pg_table, destination)

    if column_name and last_value:
        sql += f' WHERE "{column_name}" > %s'
        params += (last_value,)

    sql += f' ORDER BY "{column_name}" LIMIT 2000000'

    yield from fetch_batched(source, sql, params)


def run(table_mapping: TableMapping, pipe_line_name: str, dataset_name: str, source: str, destination: str) -> None:
    logging.info(f"{pipe_line_name} pipeline started")
    start = time.time()

    pipe = dlt.pipeline(
        pipeline_name=pipe_line_name,
        destination=destination,
        dataset_name=dataset_name,
    )

    streams = []
    for table, (_, columns) in table_mapping.items():
        resource = _stream_table(pg_table=table, source=source, destination=destination)
        if columns:
            resource.apply_hints(columns=columns)
        streams.append(resource)

    pipe.run(streams)

    logging.info("Run finished in %.2f seconds", time.time() - start)
