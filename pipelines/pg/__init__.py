from pipelines.pg.dashboard import all_tables as dashboard_all_tables
from pipelines.pg.marine import all_tables as marine_all_tables
from pipelines.pg.travel import common_tables, log_table
from pipelines.pg.db_utils import preflight, run_clickhouse_post_dlt_cleanup


def run(db: str) -> None:
    if db == "travel":
        preflight("pg_replication", "clickhouse")
        log_table.run()
        common_tables.run()

        # run_clickhouse_post_dlt_cleanup()
    elif db == "dashboard":
        preflight("pg_dashboard", "clickhouse_dashboard")
        dashboard_all_tables.run()
    elif db == "marine":
        preflight("pg_marine", "clickhouse_marine")
        marine_all_tables.run()
