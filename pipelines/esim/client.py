import logging
import time
from typing import Any, Iterator

import requests

from pipelines.esim.constants import (
    DEFAULT_LIMIT,
    MAX_PAGES,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
    RETRY_BACKOFF_BASE,
)

logger = logging.getLogger(__name__)


def _build_url(base_url: str, endpoint: str) -> str:
    return f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"


def _extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    items = payload.get("items")
    if not isinstance(items, list):
        raise ValueError("Response payload does not contain an 'items' list.")
    return items


def fetch_all_pages(
    base_url: str,
    endpoint: str,
    api_key: str,
    updated_after: str | None = None,
    limit: int = DEFAULT_LIMIT,
) -> Iterator[dict[str, Any]]:
    session = requests.Session()
    session.headers.update(
        {
            "Accept": "application/json",
            "X-Internal-API-Key": api_key,
        }
    )

    url = _build_url(base_url, endpoint)
    cursor: str | None = None
    seen_cursors: set[str] = set()
    page_count = 0

    while page_count < MAX_PAGES:
        params: dict[str, Any] = {"limit": limit}
        if cursor:
            params["cursor"] = cursor
        elif updated_after:
            params["updated_after"] = updated_after

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)

                response.raise_for_status()
                break
            except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as exc:
                status_code = getattr(getattr(exc, "response", None), "status_code", None)
                is_retryable_http = status_code == 429 or (
                    status_code is not None and 500 <= status_code < 600
                )
                is_retryable = is_retryable_http or isinstance(
                    exc, (requests.ConnectionError, requests.Timeout)
                )

                if not is_retryable or attempt >= MAX_RETRIES:
                    logger.exception(
                        "Request failed for %s with params=%s after %s attempt(s)",
                        endpoint,
                        params,
                        attempt,
                    )
                    raise

                sleep_seconds = RETRY_BACKOFF_BASE ** (attempt - 1)
                logger.warning(
                    "Retrying %s after attempt %s failed with status=%s; sleeping %ss",
                    endpoint,
                    attempt,
                    status_code,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)

        payload = response.json()
        items = _extract_items(payload)
        has_more = payload.get("has_more", False)
        next_cursor = payload.get("next_cursor")

        page_count += 1
        logger.info(
            "Fetched esim export page %s for %s with %s item(s)",
            page_count,
            endpoint,
            len(items),
        )

        for item in items:
            yield item

        if not has_more or not next_cursor:
            return

        if next_cursor in seen_cursors:
            logger.warning(
                "Stopping pagination for %s because cursor %s repeated",
                endpoint,
                next_cursor,
            )
            return

        seen_cursors.add(next_cursor)
        cursor = next_cursor

    logger.warning("Stopping pagination for %s after reaching MAX_PAGES=%s", endpoint, MAX_PAGES)
