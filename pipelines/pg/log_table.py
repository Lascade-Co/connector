from constants import LOG_TABLE
from pg_replication import replication_resource
from pg_replication.helpers import init_replication
from pipelines.pg.db_utils import get_pipeline
import logging, sys, dlt

from pipelines.pg.parsers import legacy_inline_ad, car_ads, flight_ads, hotel_ads

SLOT_INLINE_ADS = "slot_inline_ads"
SLOT_AD_FETCH = "slot_ad_fetch"


def pg_resource(slot, publication, table_name):
    res = replication_resource(slot, publication)
    res.name = table_name  # => ClickHouse table name
    return res


# ----------------------------  transformers  --------------------------------
#
#  Here is where you turn the JSON into flat columns.
#  The preprocessing logic is up to you – the resource already carries only
#  the relevant rows and columns.

@dlt.transformer(write_disposition="append", primary_key="id")
def inline_ads(rows=pg_resource(SLOT_INLINE_ADS, "pub_inline_ads", "inline_ads")):
    for row in rows:
        if row["name"].endswith("car"):
            parser = car_ads
        elif row["name"].endswith("flight"):
            parser = flight_ads
        elif row["name"].endswith("hotel"):
            parser = hotel_ads
        else:
            logging.warning("Unknown ad type: %s", row["name"])
            continue

        for ad in parser(row):
            yield ad


@dlt.transformer(write_disposition="append", primary_key="id")
def inline_ads_legacy(rows=pg_resource(SLOT_AD_FETCH, "pub_ad_fetch", "inline_ads_legacy")):
    for row in rows:
        for ad in legacy_inline_ad(row):
            yield ad


# ----------------------------  pipeline entry  ------------------------------

def run() -> None:
    pipe = get_pipeline("logs_to_click")

    if pipe.first_run:
        logging.info("Taking filtered initial snapshots")
        snap_ads = init_replication(
            slot_name=SLOT_INLINE_ADS,
            pub_name="pub_inline_ads",
            schema_name="public",
            table_names=[LOG_TABLE],
            persist_snapshots=True,
            reset=True,
        )
        snap_fetch = init_replication(
            slot_name=SLOT_AD_FETCH,
            pub_name="pub_ad_fetch",
            schema_name="public",
            table_names=[LOG_TABLE],
            persist_snapshots=True,
            reset=True,
        )
        pipe.run([inline_ads(rows=snap_ads), inline_ads_legacy(rows=snap_fetch)])

    logging.info("Streaming logical changes …")
    pipe.run([inline_ads, inline_ads_legacy])


if __name__ == "__main__":
    try:
        run()
    except SystemExit as exc:
        logging.error("❌ %s", exc)
        sys.exit(1)
    except Exception:
        logging.exception("Unhandled pipeline error", exc_info=True)
        sys.exit(2)
