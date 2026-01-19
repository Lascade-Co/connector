from pipelines.pg import stream_tables
from pipelines.pg.dashboard.constants import SELECTED_TABLES
from utils import setup_logging


def run() -> None:
    stream_tables.run(
        SELECTED_TABLES,
        "dashboard_to_click",
        "dashboard",
        "pg_dashboard",
        "clickhouse_dashboard"
    )


if __name__ == "__main__":
    setup_logging()
    run()
