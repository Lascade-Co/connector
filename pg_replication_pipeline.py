import logging
import sys
import psycopg2
import psycopg2.extras
from clickhouse_connect import get_client
import dlt

from constants import SELECTED_TABLES
from pg_replication import replication_resource
from pg_replication.helpers import init_replication
from utils import _load_secrets

PG, CH = _load_secrets()  # ← credentials ready for use

SLOT = "analytics_slot"
PUB = "analytics_pub"
SCHEMA = "public"  # Postgres schema to replicate

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s │ %(message)s", stream=sys.stdout
)


def _check_pg() -> None:
    """Connectivity + logical-replication prerequisites."""
    pg_cfg = {
        "host": PG["host"],
        "port": PG["port"],
        "user": PG["username"],
        "password": PG["password"],
        "database": PG["database"],
    }
    with psycopg2.connect(**pg_cfg, cursor_factory=psycopg2.extras.DictCursor) as cx:
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
    client = get_client(
        host=CH["host"],
        port=CH["http_port"],
        username=CH["username"],
        password=CH["password"],
        database=CH["database"],
        secure=CH["secure"],
    )
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


def get_pipeline() -> dlt.Pipeline:
    return dlt.pipeline(
        pipeline_name="pg_to_click",
        destination="clickhouse",
        dataset_name=CH["database"],
    )


def main() -> None:
    preflight()

    pipe = get_pipeline()

    if pipe.first_run:
        logging.info("First run – taking initial snapshot")
        snapshot = init_replication(
            slot_name=SLOT,
            pub_name=PUB,
            schema_name=SCHEMA,
            table_names=SELECTED_TABLES,
            persist_snapshots=True,
            reset=True
        )

        logging.info("Snapshot complete, starting replication")

        pipe.run(snapshot)

    logging.info("Streaming logical changes …")
    pipe.run(replication_resource(SLOT, PUB))
    logging.info("Run finished")


if __name__ == "__main__":
    try:
        main()
    except SystemExit as exc:
        logging.error("❌  %s", exc)
        sys.exit(1)
    except Exception as e:
        logging.exception(f"Unhandled pipeline error {e}", exc_info=True)
        sys.exit(2)
