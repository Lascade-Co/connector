import sys
import logging
import dlt

from constants import LOG_TABLE
from pipelines.pg.db_utils import get_pipeline
from pipelines.pg.parsers import legacy_inline_ad, car_ads, flight_ads, hotel_ads

DESTINATION = "inline_ad_logs"


@dlt.resource(
    standalone=True,
    name=DESTINATION,
    write_disposition="merge",
    merge_key="id",
    primary_key="id",
)
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

    pipe = get_pipeline("inline_ad_logs_to_click")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s │ %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )
    run()
