from pipelines.pg import stream_tables
from pipelines.pg.travel.constants import TABLE_TO_FIELD_MAPPING
from utils import setup_logging


def run() -> None:
    stream_tables.run(
        TABLE_TO_FIELD_MAPPING,
        "pg_to_click",
        "travel",
        "pg_replication",
        "clickhouse"
    )


if __name__ == "__main__":
    setup_logging()
    run()
