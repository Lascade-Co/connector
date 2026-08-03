"""Stage additive order analytics fields without rewriting the orders table."""

import logging
import os
import sys
from typing import Any, Iterator, MutableMapping

import dlt

from pipelines.esim.client import fetch_all_pages
from pipelines.esim.constants import DATASET_COLUMN_HINTS
from pipelines.esim.manifest import fetch_manifest, parse_manifest
from utils import load_config

RESOURCE_NAME = "orders_analytics_fields_1_2_backfill"
BACKFILL_FIELDS = (
    "id",
    "subtotal_eur",
    "profit_eur",
    "item_count",
    "distinct_plan_count",
    "is_cart",
)


def select_backfill_fields(item: dict[str, Any]) -> dict[str, Any]:
    missing = [field for field in BACKFILL_FIELDS if field not in item]
    if missing:
        raise ValueError(
            "Orders export is missing schema 1.2 backfill fields: " + ", ".join(missing)
        )
    return {field: item[field] for field in BACKFILL_FIELDS}


@dlt.resource(
    name=RESOURCE_NAME,
    primary_key="id",
    write_disposition="replace",
    columns={
        "id": {"data_type": "text", "nullable": False},
        **DATASET_COLUMN_HINTS["orders"],
    },
)
def order_fields_resource(
    *,
    base_url: str,
    api_key: str,
    endpoint: str,
    limit: int,
    metrics: MutableMapping[str, Any],
) -> Iterator[dict[str, Any]]:
    for item in fetch_all_pages(
        base_url=base_url,
        endpoint=endpoint,
        api_key=api_key,
        limit=limit,
    ):
        row = select_backfill_fields(item)
        if row["id"] in metrics["ids"]:
            raise ValueError(f"Duplicate order ID in backfill source: {row['id']}")
        metrics["ids"].add(row["id"])
        yield row


def validate_staged_backfill(pipeline, expected_count: int) -> dict[str, int]:
    """Validate the staging table and its ID coverage without mutating orders."""
    with pipeline.sql_client() as client:
        staging_table = client.make_qualified_table_name(RESOURCE_NAME)
        orders_table = client.make_qualified_table_name("orders")
        counts = client.execute_sql(
            f"""
            SELECT
                count(),
                uniqExact(id),
                countIf(subtotal_eur IS NULL),
                countIf(item_count IS NULL),
                countIf(distinct_plan_count IS NULL),
                countIf(is_cart IS NULL),
                countIf(profit_eur IS NULL AND item_count > 0)
            FROM {staging_table}
            """
        )[0]
        missing_from_staging = client.execute_sql(
            f"""
            SELECT count()
            FROM (SELECT id FROM {orders_table} FINAL) AS existing
            LEFT ANTI JOIN {staging_table} AS staged USING (id)
            """
        )[0][0]
        extra_in_staging = client.execute_sql(
            f"""
            SELECT count()
            FROM {staging_table} AS staged
            LEFT ANTI JOIN (SELECT id FROM {orders_table} FINAL) AS existing USING (id)
            """
        )[0][0]

    stats = {
        "expected_count": expected_count,
        "row_count": counts[0],
        "unique_id_count": counts[1],
        "null_subtotal_count": counts[2],
        "null_item_count": counts[3],
        "null_distinct_plan_count": counts[4],
        "null_is_cart_count": counts[5],
        "unexpected_null_profit_count": counts[6],
        "missing_from_staging": missing_from_staging,
        "extra_in_staging": extra_in_staging,
    }
    failures = {key: value for key, value in stats.items() if key not in {"expected_count", "row_count", "unique_id_count"} and value}
    if stats["row_count"] != expected_count:
        failures["row_count"] = stats["row_count"]
    if stats["unique_id_count"] != expected_count:
        failures["unique_id_count"] = stats["unique_id_count"]
    if failures:
        raise RuntimeError(f"Order analytics backfill validation failed: {failures}")
    return stats


def run() -> None:
    if len(sys.argv) < 3 or not sys.argv[2]:
        raise ValueError("Please provide a group name as the second argument.")

    group_name = sys.argv[2]
    config = load_config(group_name, "esim")
    datasets = parse_manifest(
        fetch_manifest(base_url=config["base_url"], api_key=config["api_key"])
    )
    orders = next(dataset for dataset in datasets if dataset["name"] == "orders")
    if orders["schema_version"] != "1.2":
        raise RuntimeError("Orders schema 1.2 must be deployed before staging its backfill.")

    suffix = os.getenv("PIPELINE_NAME_SUFFIX", "")
    pipeline = dlt.pipeline(
        pipeline_name=f"esim_order_fields_{group_name}{suffix}",
        destination=dlt.destinations.clickhouse(destination_name="clickhouse_esim"),
        dataset_name="esim",
    )
    metrics: dict[str, Any] = {"ids": set()}
    load_info = pipeline.run(
        order_fields_resource(
            base_url=config["base_url"],
            api_key=config["api_key"],
            endpoint=orders["endpoint"],
            limit=orders["default_limit"],
            metrics=metrics,
        )
    )
    logging.info("Staged additive order analytics fields: %s", load_info)
    validation = validate_staged_backfill(pipeline, expected_count=len(metrics["ids"]))
    logging.info("Validated staged additive order analytics fields: %s", validation)


if __name__ == "__main__":
    run()
