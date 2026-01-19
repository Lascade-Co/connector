from pipelines.pg.dashboard import all_tables
from pipelines.pg.travel import common_tables, log_table
from pipelines.pg.db_utils import preflight, run_clickhouse_post_dlt_cleanup


def run() -> None:
    preflight("pg_replication", "clickhouse")

    log_table.run()
    common_tables.run()

    run_clickhouse_post_dlt_cleanup()

    preflight("pg_dashboard", "clickhouse_dashboard")

    all_tables.run()
