from typing import Any, Iterator

import dlt

from pipelines.esim.client import fetch_all_pages
from pipelines.esim.constants import DATASETS


def make_resource(
    dataset_name: str,
    config: dict[str, str],
    base_url: str,
    api_key: str,
):
    watermark_field = config["watermark_field"]
    endpoint = config["endpoint"]
    primary_key = config["primary_key"]
    write_disposition = config["write_disposition"]

    columns = config.get("columns")

    @dlt.resource(
        name=dataset_name,
        primary_key=primary_key,
        write_disposition=write_disposition,
        columns=columns,
    )
    def resource(
        updated_after: dlt.sources.incremental[str] = dlt.sources.incremental(
            watermark_field,
            initial_value=None,
        )
    ) -> Iterator[dict[str, Any]]:
        yield from fetch_all_pages(
            base_url=base_url,
            endpoint=endpoint,
            api_key=api_key,
            updated_after=updated_after.last_value,
        )

    return resource


@dlt.source(name="esim_analytics")
def esim_analytics(base_url: str, api_key: str):
    for dataset_name, config in DATASETS.items():
        yield make_resource(
            dataset_name=dataset_name,
            config=config,
            base_url=base_url,
            api_key=api_key,
        )()
