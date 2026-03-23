from typing import Any, Iterator

import dlt

from pipelines.esim.client import fetch_all_pages


def make_incremental_resource(
    dataset_name: str,
    config: dict[str, Any],
    base_url: str,
    api_key: str,
    endpoint: str,
):
    watermark_field = config["watermark_field"]
    primary_key = config["primary_key"]
    write_disposition = config["write_disposition"]
    limit = config["default_limit"]

    @dlt.resource(
        name=dataset_name,
        primary_key=primary_key,
        write_disposition=write_disposition,
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
            limit=limit,
        )

    return resource


def make_full_refresh_resource(
    dataset_name: str,
    config: dict[str, Any],
    base_url: str,
    api_key: str,
    endpoint: str,
):
    write_disposition = config["write_disposition"]
    limit = config["default_limit"]

    @dlt.resource(
        name=dataset_name,
        write_disposition=write_disposition,
    )
    def resource() -> Iterator[dict[str, Any]]:
        yield from fetch_all_pages(
            base_url=base_url,
            endpoint=endpoint,
            api_key=api_key,
            limit=limit,
        )

    return resource


@dlt.source(name="esim_analytics")
def esim_analytics(base_url: str, api_key: str, datasets: list[dict[str, Any]]):
    for config in datasets:
        dataset_name = config["name"]
        endpoint = config["endpoint"]
        resource_factory = (
            make_incremental_resource
            if config.get("watermark_field") is not None
            else make_full_refresh_resource
        )
        yield resource_factory(
            dataset_name=dataset_name,
            config=config,
            base_url=base_url,
            api_key=api_key,
            endpoint=endpoint,
        )()
