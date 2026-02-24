import json
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
def _stream_table(pg_table: str, source: str, destination: str, json_columns: list[str]):
    # Build a source query with a parameterized timestamp filter
    sql = f"SELECT * FROM {pg_table}"
    params = ()
    column_name, last_value = get_last_record_info(pg_table, destination)

    if column_name and last_value:
        sql += f' WHERE "{column_name}" > %s'
        params += (last_value,)

    sql += f' ORDER BY "{column_name}" LIMIT 2000000'

    for row in fetch_batched(source, sql, params):
        for col in json_columns:
            if col in row and isinstance(row[col], (dict, list)):
                row[col] = json.dumps(row[col])
        yield row


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
        json_columns = [col for col, spec in (columns or {}).items() if spec.get('data_type') == 'json']
        resource = _stream_table(pg_table=table, source=source, destination=destination, json_columns=json_columns)
        if columns:
            resource.apply_hints(columns=columns)
        streams.append(resource)

    pipe.run(streams, refresh="drop_sources")

    logging.info("Run finished in %.2f seconds", time.time() - start)
