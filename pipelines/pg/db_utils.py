import logging
import psycopg2
import psycopg2.extras
from clickhouse_connect import get_client
import dlt
from clickhouse_connect.driver import Client

from utils import _load_secrets

PG, CH = _load_secrets()  # ← credentials ready for use

def get_pg_connection() -> psycopg2.extensions.connection:
    """Return a connection to PostgreSQL."""
    pg_cfg = {
        "host": PG["host"],
        "port": PG["port"],
        "user": PG["username"],
        "password": PG["password"],
        "database": PG["database"],
    }
    conn =  psycopg2.connect(**pg_cfg, cursor_factory=psycopg2.extras.DictCursor)
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
    with get_pg_connection() as cx:
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
