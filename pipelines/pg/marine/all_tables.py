from pipelines.pg import stream_tables
from pipelines.pg.marine.constants import TABLE_TO_FIELD_MAPPING
from utils import setup_logging


def run() -> None:
    stream_tables.run(
        TABLE_TO_FIELD_MAPPING,
        "marine_to_click",
        "marine",
        "pg_marine",
        "clickhouse_marine"
    )


if __name__ == "__main__":
    setup_logging()
    run()
