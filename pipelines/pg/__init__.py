from pipelines.pg import common_tables, log_table
from pipelines.pg.db_utils import preflight


def run() -> None:
    preflight()

    log_table.run()
    common_tables.run()
