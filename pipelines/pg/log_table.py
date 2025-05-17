import sys
import logging
import dlt

from constants import LOG_TABLE
from pg_replication import replication_resource
from pg_replication.helpers import init_replication
from pipelines.pg.db_utils import get_pipeline
from pipelines.pg.parsers import legacy_inline_ad, car_ads, flight_ads, hotel_ads

SLOT = "slot_inline_ads"
PUB = "pub_inline_ads"
SCHEMA = "public"


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

        yield from parser(row)  # parser returns one or many dicts


def run() -> None:
    logging.info("Logs to ClickHouse pipeline started")

    pipe = get_pipeline("logs_to_click")

    if pipe.first_run:
        logging.info("Taking filtered initial snapshots")
        snap_ads = init_replication(
            slot_name=SLOT,
            pub_name=PUB,
            schema_name=SCHEMA,
            table_names=[LOG_TABLE],
            persist_snapshots=True,
            reset=True,
        )

        logging.info("Taking initial snapshots")

        pipe.run(snap_ads | inline_ads)

    logging.info("Streaming logical changes …")

    replication = replication_resource(SLOT, PUB)
    replication.apply_hints(write_disposition="skip")

    pipe.run(replication | inline_ads)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s │ %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    run()
