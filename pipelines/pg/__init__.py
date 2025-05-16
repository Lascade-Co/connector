from pipelines.pg import common_tables, log_table
from pipelines.pg.db_utils import preflight


def run() -> None:
    preflight()

    common_tables.run()
    log_table.run()
