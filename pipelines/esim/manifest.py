import logging
import time
from typing import Any

import requests

from pipelines.esim.constants import (
    DEFAULT_LIMIT,
    DEFAULT_LIMIT_MAX,
    DEFAULT_LIMIT_MIN,
    MANIFEST_ENDPOINT,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    RETRY_BACKOFF_BASE,
    STRATEGY_TO_DISPOSITION,
)

logger = logging.getLogger(__name__)


def _build_url(base_url: str, endpoint: str) -> str:
    return f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"


def _extract_datasets(payload: dict[str, Any]) -> list[dict[str, Any]]:
    datasets = payload.get("datasets")
    if not isinstance(datasets, list):
        raise ValueError("Manifest response does not contain a 'datasets' list.")
    return datasets


def _request_manifest(
    session: requests.Session,
    url: str,
) -> requests.Response:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
            status_code = getattr(getattr(exc, "response", None), "status_code", None)
            is_retryable_http = status_code == 429 or (
                status_code is not None and 500 <= status_code < 600
            )
            is_retryable = is_retryable_http or isinstance(
                exc, (requests.ConnectionError, requests.Timeout)
            )

            if not is_retryable or attempt >= MAX_RETRIES:
                raise RuntimeError(
                    f"Failed to fetch eSIM manifest from {url} after {attempt} attempt(s)."
                ) from exc

            sleep_seconds = RETRY_BACKOFF_BASE ** (attempt - 1)
            logger.warning(
                "Retrying manifest fetch after attempt %s failed with status=%s; sleeping %ss",
                attempt,
                status_code,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(f"Failed to fetch eSIM manifest from {url}.")


def fetch_manifest(base_url: str, api_key: str) -> list[dict]:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "X-Internal-API-Key": api_key,
        }
    )

    url = _build_url(base_url, MANIFEST_ENDPOINT)
    response = _request_manifest(session, url)
    payload = response.json()
    return _extract_datasets(payload)


def _require_non_empty_string(dataset: dict[str, Any], key: str, dataset_name: str) -> str:
    value = dataset.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Dataset '{dataset_name}' has invalid '{key}'.")
    return value.strip()


def _resolve_watermark_field(dataset: dict[str, Any], strategy: str, dataset_name: str) -> str | None:
    if strategy in {"full-refresh", "full-refresh-snapshot"}:
        return None
    value = dataset.get("watermark_field")
    if not isinstance(value, str) or not value.strip():
        raise ValueError(
            f"Dataset '{dataset_name}' requires a non-empty 'watermark_field' for strategy '{strategy}'."
        )
    return value.strip()


def _resolve_default_limit(dataset: dict[str, Any]) -> int:
    value = dataset.get("default_limit")
    if isinstance(value, bool) or not isinstance(value, int):
        return DEFAULT_LIMIT
    return max(DEFAULT_LIMIT_MIN, min(value, DEFAULT_LIMIT_MAX))


def _normalize_dataset(dataset: dict[str, Any]) -> dict[str, Any]:
    original_name = _require_non_empty_string(dataset, "name", "<unknown>")
    strategy = _require_non_empty_string(dataset, "strategy", original_name)
    if strategy not in STRATEGY_TO_DISPOSITION:
        raise ValueError(f"Dataset '{original_name}' has unknown strategy '{strategy}'.")

    normalized_name = original_name.replace("-", "_")
    endpoint = _require_non_empty_string(dataset, "endpoint", original_name)
    watermark_field = _resolve_watermark_field(dataset, strategy, original_name)
    is_full_refresh = strategy in {"full-refresh", "full-refresh-snapshot"}
    primary_key = None if is_full_refresh else _require_non_empty_string(dataset, "primary_key", original_name)

    resolved = {
        "name": normalized_name,
        "endpoint": endpoint,
        "watermark_field": watermark_field,
        "primary_key": primary_key,
        "write_disposition": STRATEGY_TO_DISPOSITION[strategy],
        "default_limit": _resolve_default_limit(dataset),
        "schema_version": dataset.get("schema_version"),
    }
    logger.info("Resolved manifest dataset config: %s", resolved)
    return resolved


def parse_manifest(raw_datasets: list[dict]) -> list[dict]:
    available_datasets = [dataset for dataset in raw_datasets if dataset.get("available") is True]
    if not available_datasets:
        raise ValueError("Manifest does not contain any available datasets.")

    parsed_datasets = [_normalize_dataset(dataset) for dataset in available_datasets]
    seen_names: set[str] = set()
    duplicate_names: set[str] = set()
    for dataset in parsed_datasets:
        name = dataset["name"]
        if name in seen_names:
            duplicate_names.add(name)
        seen_names.add(name)
    if duplicate_names:
        raise ValueError(
            "Manifest contains duplicate normalized dataset names: "
            f"{', '.join(sorted(duplicate_names))}."
        )

    return sorted(parsed_datasets, key=lambda dataset: dataset["name"])
