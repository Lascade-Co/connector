from pipelines.pg import dashboard
from pipelines.pg import marine
from pipelines.pg.travel import common_tables, log_table
from pipelines.pg.db_utils import preflight, run_clickhouse_post_dlt_cleanup


def run(db: str) -> None:
    if db == "travel":
        preflight("pg_replication", "clickhouse")
        log_table.run()
        common_tables.run()

        run_clickhouse_post_dlt_cleanup()
    elif db == "dashboard":
        preflight("pg_dashboard", "clickhouse_dashboard")
        dashboard.all_tables.run()
    elif db == "marine":
        preflight("pg_marine", "clickhouse_marine")
        marine.all_tables.run()
