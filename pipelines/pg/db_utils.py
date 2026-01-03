import logging
import os

import psycopg2
import psycopg2.extras
from clickhouse_connect import get_client
import dlt
from clickhouse_connect.driver import Client
from constants import TABLE_TO_FIELD_MAPPING
from utils import _load_secrets

PG, CH = _load_secrets()  # ← credentials ready for use


def get_pg_connection(real_dict=True) -> psycopg2.extensions.connection:
    """Return a connection to PostgreSQL."""
    pg_cfg = {
        "host": PG["host"],
        "port": PG["port"],
        "user": PG["username"],
        "password": PG["password"],
        "database": PG["database"],
    }
    conn = psycopg2.connect(**pg_cfg, cursor_factory=psycopg2.extras.DictCursor)

    if real_dict:
        conn.cursor_factory = psycopg2.extras.RealDictCursor

    return conn


def get_ch_connection() -> Client:
    """Return a connection to ClickHouse."""
    return get_client(
        host=CH["host"],
        port=CH["http_port"],
        username=CH["username"],
        password=CH["password"],
        database=CH["database"],
        secure=CH["secure"],
    )


def _check_pg() -> None:
    """Connectivity + logical-replication prerequisites."""
    with get_pg_connection(False) as cx:
        cx.autocommit = True
        cur = cx.cursor()

        cur.execute("SHOW wal_level;")
        wal_level = cur.fetchone()[0]
        if wal_level.lower() != "logical":
            raise SystemExit(
                f"wal_level is '{wal_level}', must be 'logical' for logical decoding"
            )

        cur.execute(
            """
            SELECT rolname, rolsuper, rolreplication
            FROM pg_roles
            WHERE rolname = current_user;
            """
        )
        role = cur.fetchone()
        if not role or (not role["rolsuper"] and not role["rolreplication"]):
            raise SystemExit(
                "Current Postgres role lacks REPLICATION privilege or superuser"
            )

        logging.info("✓ PostgreSQL connectivity and privileges verified")


def _check_clickhouse() -> None:
    """Connectivity + INSERT privilege to ClickHouse."""
    client = get_ch_connection()

    client.query("SELECT 1")
    logging.info("✓ ClickHouse connectivity verified")

    exists = client.query(
        "EXISTS DATABASE {db:Identifier}", parameters={"db": CH["database"]}
    ).first_item

    if not exists:
        raise SystemExit(
            f"Database {CH['database']} does not exist in ClickHouse, please create it"
        )

    logging.info(f"✓ ClickHouse database '{CH['database']}' exists")

    client.command("CREATE TEMPORARY TABLE _permcheck (x UInt8) ENGINE = Memory;")
    logging.info("✓ Table creation privilege verified")

    client.command("INSERT INTO _permcheck VALUES (1);")
    logging.info("✓ Table insert privilege verified")

    client.command("DROP TABLE _permcheck")
    logging.info("✓ Temporary table dropped")

    grant = client.command("CHECK GRANT SELECT ON INFORMATION_SCHEMA.*;")
    if not grant:
        raise SystemExit(
            "Current ClickHouse role lacks SELECT privilege on INFORMATION_SCHEMA"
        )
    logging.info("✓ SELECT privilege on INFORMATION_SCHEMA verified")

    client.close()


def _get_destination_table_name(table: str) -> str:
    """Get the destination table name for the given source table name."""
    return f"{CH['database']}___{table}"


def _get_last_for_column(pg_table: str, column: str) -> str | None:
    ch_client = get_ch_connection()
    destination = _get_destination_table_name(pg_table)

    # Check if the destination table exists; if not, full initial load
    exists_count = ch_client.query(
        "SELECT count() as cnt FROM system.tables WHERE database = %s AND name = %s",
        (CH['database'], destination)
    ).first_item["cnt"]

    if exists_count > 0:
        # Get max(created_at) from ClickHouse (timezone preserved by driver)
        try:
            return ch_client.query(
                f"SELECT MAX({column}) as last FROM `{CH['database']}`.`{destination}`"
            ).first_item["last"]
        except Exception:
            pass

    ch_client.close()
    return None


def get_last_id(pg_table: str) -> str | None:
    return _get_last_for_column(pg_table, "id")


def get_last_created_at(pg_table: str) -> str | None:
    return _get_last_for_column(pg_table, "created_at")


def get_last_record_info(pg_table: str) -> tuple[str, str | None]:
    column_name = TABLE_TO_FIELD_MAPPING.get(pg_table, "id")
    return column_name, _get_last_for_column(pg_table, column_name)


def fetch_batched(query: str, params: tuple, batch_size: int = 4000):
    with get_pg_connection() as conn:
        # Batch fetching to limit memory footprint
        with conn.cursor() as cur:
            cur.itersize = batch_size
            cur.execute(query, params)
            while True:
                batch = cur.fetchmany(batch_size)
                if not batch:
                    break
                for row in batch:
                    yield row


def preflight() -> None:
    logging.info("Running environment validation …")
    _check_pg()
    _check_clickhouse()
    logging.info("Environment validation complete.\n")


def get_pipeline(name: str) -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name=name,
        destination="clickhouse",
        dataset_name=CH["database"],
    )


def run_clickhouse_post_dlt_cleanup() -> None:
    if os.getenv("SKIP_CH_CLEANUP"):
        logging.info("Skipping ClickHouse cleanup (SKIP_CH_CLEANUP is set)")
        return

    cleanup_statements = (
        "delete from travel.travel___users_conversions where updated_at>=(select min(updated_at) from travel.travel___users_conversions where id in (select conversion_id from travel.travel___conversions_enriched group by conversion_id having count()>1));",
        "delete from travel.travel___conversions_enriched where conversion_id not in (select id from travel.travel___users_conversions);",
        "delete from travel.travel___users_usersession where id >= (select min(session_id) from travel.travel___user_sessions_enriched where app_vertical is null);",
        "delete from travel.travel___user_sessions_enriched where session_id not in (select id from travel.travel___users_usersession);",
    )

    client = get_ch_connection()
    try:
        logging.info("Running ClickHouse cleanup mutations...")
        for stmt in cleanup_statements:
            client.command(stmt)
        logging.info("ClickHouse cleanup mutations finished")
    finally:
        client.close()
