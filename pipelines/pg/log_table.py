import sys

from constants import LOG_TABLE
from pg_replication import replication_resource
from pg_replication.helpers import init_replication
from pipelines.pg.db_utils import get_pipeline
import logging, dlt

from pipelines.pg.parsers import legacy_inline_ad, car_ads, flight_ads, hotel_ads

SLOT_INLINE_ADS = "slot_inline_ads"
PUB_INLINE_ADS = "pub_inline_ads"


# ----------------------------  transformers  --------------------------------
#
#  Here is where you turn the JSON into flat columns.
#  The preprocessing logic is up to you – the resource already carries only
#  the relevant rows and columns.

@dlt.transformer(write_disposition="append", primary_key="id")
def inline_ads(rows):
    for row in rows:
        if row["name"].endswith("car"):
            parser = car_ads
        elif row["name"].endswith("flight"):
            parser = flight_ads
        elif row["name"].endswith("hotel"):
            parser = hotel_ads
        elif row["name"].endswith("ad_fetch"):
            parser = legacy_inline_ad
        else:
            logging.warning("Unknown ad type: %s", row["name"])
            continue

        for ad in parser(row):
            yield ad


# ----------------------------  pipeline entry  ------------------------------

def run() -> None:
    logging.info("Logs to ClickHouse pipeline started")

    pipe = get_pipeline("logs_to_click")

    if pipe.first_run:
        logging.info("Taking filtered initial snapshots")
        snap_ads = init_replication(
            slot_name=SLOT_INLINE_ADS,
            pub_name=PUB_INLINE_ADS,
            schema_name="public",
            table_names=[LOG_TABLE],
            persist_snapshots=True,
            reset=True,
        )

        logging.info("Taking initial snapshots")

        pipe.run(snap_ads | inline_ads)

    logging.info("Streaming logical changes …")

    pipe.run(replication_resource(SLOT_INLINE_ADS, PUB_INLINE_ADS) | inline_ads)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s │ %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    run()
