import logging

import dlt

from constants import LOG_TABLE
from pipelines.pg.db_utils import fetch_batched, get_last_record_info
from pipelines.pg.travel.parsers import legacy_inline_ad, car_ads, flight_ads, hotel_ads
from utils import setup_logging

DESTINATION = "inline_ad_logs"


@dlt.resource(
    standalone=True,
    name=DESTINATION,
    write_disposition="merge",
    merge_key="id",
    primary_key="id",
)
def inline_ads():
    last_created_at = get_last_record_info(DESTINATION, "clickhouse")

    ad_types = ('InlineAdsViewSet.car', 'InlineAdsViewSet.flight', 'InlineAdsViewSet.hotel', 'ad_fetch')
    sql = f"SELECT * FROM {LOG_TABLE} WHERE name IN (%s, %s, %s, %s)"
    params = ad_types

    if last_created_at:
        sql += " AND created_at > %s"
        params = (*ad_types, last_created_at)

    for row in fetch_batched("pg_replication", sql, params):
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

    pipe = dlt.pipeline(
        pipeline_name="inline_ad_logs_to_click",
        destination="clickhouse",
        dataset_name="travel",
    )
    pipe.run(inline_ads())

    logging.info("Run finished")


if __name__ == "__main__":
    setup_logging()
    run()
