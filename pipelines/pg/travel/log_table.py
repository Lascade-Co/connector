import logging
import time

import dlt

from pipelines.pg.travel.constants import LOG_TABLE
from pipelines.pg.db_utils import fetch_batched, get_last_logs_record_info
from pipelines.pg.travel.parsers import ad_request_stats as parse_request, legacy_inline_ad, car_ads, flight_ads, hotel_ads
from utils import setup_logging

DESTINATION = "inline_ad_logs"
AD_REQUEST_STATS_DESTINATION = "ad_request_stats"


@dlt.resource(
    standalone=True,
    name=DESTINATION,
    write_disposition="merge",
    merge_key="id",
    primary_key="id",
)
def inline_ads():
    column, last_record = get_last_logs_record_info(DESTINATION, "clickhouse")

    ad_types = ('InlineAdsViewSet.car', 'InlineAdsViewSet.flight', 'InlineAdsViewSet.hotel', 'ad_fetch')
    sql = f"SELECT * FROM {LOG_TABLE} WHERE name IN (%s, %s, %s, %s)"
    params = ad_types

    logging.info(f"Last record: {column}={last_record}")

    if last_record:
        sql += f' AND "{column}" > %s ORDER BY "{column}"'
        params = (*ad_types, last_record)

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


@dlt.resource(
    standalone=True,
    name=AD_REQUEST_STATS_DESTINATION,
    write_disposition="merge",
    merge_key="id",
    primary_key="id",
)
def ad_request_stats():
    column, last_record = get_last_logs_record_info(AD_REQUEST_STATS_DESTINATION, "clickhouse")

    ad_types = ('InlineAdsViewSet.car', 'InlineAdsViewSet.flight', 'InlineAdsViewSet.hotel', 'ad_fetch')
    sql = f"SELECT * FROM {LOG_TABLE} WHERE name IN (%s, %s, %s, %s)"
    params = ad_types

    logging.info(f"Last record: {column}={last_record}")

    if last_record:
        sql += f' AND "{column}" > %s ORDER BY "{column}"'
        params = (*ad_types, last_record)

    for row in fetch_batched("pg_replication", sql, params):
        yield parse_request(row)


def run() -> None:
    logging.info("Logs to ClickHouse pipeline started")
    start = time.time()

    pipe = dlt.pipeline(
        pipeline_name="inline_ad_logs_to_click",
        destination="clickhouse",
        dataset_name="travel",
    )
    pipe.run(inline_ads())
    pipe.run(ad_request_stats())

    logging.info("Run finished in %.2f seconds", time.time() - start)


if __name__ == "__main__":
    setup_logging()
    run()
