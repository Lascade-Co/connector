import logging
import time
from typing import Any

import requests

from pipelines.esim.constants import (
    DEFAULT_LIMIT,
    DEFAULT_LIMIT_MAX,
    DEFAULT_LIMIT_MIN,
    DATASET_COLUMN_HINTS,
    DATASET_COLUMN_HINT_MIN_VERSIONS,
    MANIFEST_ENDPOINT,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    RETRY_BACKOFF_BASE,
    STRATEGY_TO_DISPOSITION,
    SUPPORTED_SCHEMA_VERSIONS,
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


def _parse_schema_version(value: Any) -> tuple[int, int] | None:
    """Split a MAJOR.MINOR version, or None if it is not in that shape."""
    if not isinstance(value, str):
        return None
    parts = value.split(".")
    if len(parts) != 2 or not all(part.isdigit() for part in parts):
        return None
    return int(parts[0]), int(parts[1])


def _resolve_schema_version(dataset: dict[str, Any], dataset_name: str) -> str | None:
    """Gate a gated dataset's schema_version on its MAJOR, not on an exact match.

    An unreviewed MINOR of a reviewed MAJOR is additive by the backend's
    convention, so it is accepted with a warning rather than taking the whole run
    down. An unreviewed MAJOR may change or drop existing columns, so it still
    raises. See SUPPORTED_SCHEMA_VERSIONS for the full rationale.
    """
    value = dataset.get("schema_version")
    supported = SUPPORTED_SCHEMA_VERSIONS.get(dataset_name)
    if supported is None:
        return value
    if value in supported:
        return value

    reviewed = ", ".join(sorted(supported))
    parsed = _parse_schema_version(value)
    reviewed_majors = {
        parsed_supported[0]
        for parsed_supported in map(_parse_schema_version, supported)
        if parsed_supported is not None
    }
    if parsed is None or parsed[0] not in reviewed_majors:
        raise ValueError(
            f"Dataset '{dataset_name}' has unsupported schema_version '{value}'; "
            f"expected one of: {reviewed}. A new MAJOR version can change or drop "
            f"existing columns, so review the backend's export and then add the "
            f"version to SUPPORTED_SCHEMA_VERSIONS in pipelines/esim/constants.py."
        )

    logger.warning(
        "SCHEMA DRIFT: dataset '%s' reports schema_version '%s', which is not in the "
        "reviewed set (%s). Accepting it because MAJOR %s is reviewed and the backend "
        "bumps MINOR only for additive columns; dlt is inferring any new column. "
        "ACTION: review the backend's export for this version — if it added a money "
        "field, pin an explicit decimal hint in DATASET_COLUMN_HINTS (the backend sends "
        "Decimals as JSON floats, which infer as double) — then add '%s' to "
        "SUPPORTED_SCHEMA_VERSIONS in pipelines/esim/constants.py to record the review "
        "and silence this warning.",
        dataset_name,
        value,
        reviewed,
        parsed[0],
        value,
    )
    return value


def _column_hints_for_version(
    dataset_name: str,
    schema_version: str | None,
) -> dict[str, Any] | None:
    hints = DATASET_COLUMN_HINTS.get(dataset_name)
    if not hints:
        return None

    actual_version = _parse_schema_version(schema_version)
    minimum_versions = DATASET_COLUMN_HINT_MIN_VERSIONS.get(dataset_name, {})
    if actual_version is None or not minimum_versions:
        return hints

    selected = {
        column: hint
        for column, hint in hints.items()
        if (
            (minimum := _parse_schema_version(minimum_versions.get(column)))
            is None
            or actual_version >= minimum
        )
    }
    return selected or None


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
    schema_version = _resolve_schema_version(dataset, normalized_name)

    resolved = {
        "name": normalized_name,
        "endpoint": endpoint,
        "watermark_field": watermark_field,
        "primary_key": primary_key,
        "write_disposition": STRATEGY_TO_DISPOSITION[strategy],
        "default_limit": _resolve_default_limit(dataset),
        "schema_version": schema_version,
        "columns": _column_hints_for_version(normalized_name, schema_version),
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
