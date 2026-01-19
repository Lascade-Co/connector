from pipelines.pg import stream_tables
from pipelines.pg.travel.constants import SELECTED_TABLES
from utils import setup_logging


def run() -> None:
    stream_tables.run(
        SELECTED_TABLES,
        "pg_to_click",
        "travel",
        "pg_replication",
        "clickhouse"
    )


if __name__ == "__main__":
    setup_logging()
    run()
