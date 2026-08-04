"""Meta usage-header telemetry and proactive quota pacing."""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Mapping, Optional
from urllib.parse import urlsplit


RATE_LIMIT_CODES = frozenset(
    (4, 17, 32, 613, *range(80000, 80010), 80014)
)

_USAGE_HEADER_NAMES = (
    "x-business-use-case-usage",
    "x-ad-account-usage",
    "x-app-usage",
    "x-fb-ads-insights-throttle",
)

_UTILIZATION_FIELDS = (
    "call_count",
    "total_cputime",
    "total_time",
    "acc_id_util_pct",
    "app_id_util_pct",
)


@dataclass(frozen=True)
class MetaUsageSnapshot:
    """Normalized utilization data from Meta response headers."""

    utilization_pct: float
    reset_seconds: Optional[int]
    access_tier: Optional[str]
    header_names: tuple[str, ...]


def _get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logging.warning("Invalid %s=%r; defaulting to %d", name, raw, default)
        return default


def _get_header(headers: Mapping[str, str], name: str) -> Optional[str]:
    for key, value in headers.items():
        if key.lower() == name:
            return value
    return None


def _flatten_usage_entries(payload: object) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        entries.append(payload)
        for value in payload.values():
            if isinstance(value, dict):
                entries.append(value)
            elif isinstance(value, list):
                entries.extend(item for item in value if isinstance(item, dict))
    elif isinstance(payload, list):
        entries.extend(item for item in payload if isinstance(item, dict))
    return entries


def parse_usage_headers(headers: Mapping[str, str]) -> Optional[MetaUsageSnapshot]:
    """Parse all Meta quota header families without exposing credentials."""

    utilization = 0.0
    reset_seconds: Optional[int] = None
    access_tier: Optional[str] = None
    seen: list[str] = []

    for header_name in _USAGE_HEADER_NAMES:
        raw = _get_header(headers, header_name)
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            logging.warning("Meta returned malformed %s header", header_name)
            continue

        seen.append(header_name)
        for entry in _flatten_usage_entries(payload):
            for field in _UTILIZATION_FIELDS:
                value = entry.get(field)
                if isinstance(value, (int, float)):
                    utilization = max(utilization, float(value))

            regain_minutes = entry.get("estimated_time_to_regain_access")
            if isinstance(regain_minutes, (int, float)) and regain_minutes > 0:
                candidate = max(1, int(regain_minutes * 60))
                reset_seconds = max(reset_seconds or 0, candidate)

            reset_duration = entry.get("reset_time_duration")
            if isinstance(reset_duration, (int, float)) and reset_duration > 0:
                candidate = max(1, int(reset_duration))
                reset_seconds = max(reset_seconds or 0, candidate)

            tier = entry.get("ads_api_access_tier")
            if isinstance(tier, str) and tier:
                access_tier = tier

    if not seen:
        return None
    return MetaUsageSnapshot(
        utilization_pct=utilization,
        reset_seconds=reset_seconds,
        access_tier=access_tier,
        header_names=tuple(seen),
    )


def observe_meta_response(response: Any, *_args: Any, **_kwargs: Any) -> Any:
    """Log quota usage and apply only a short, header-driven proactive delay."""

    snapshot = parse_usage_headers(response.headers)
    if snapshot is None:
        return response

    request = getattr(response, "request", None)
    request_url = getattr(request, "url", "") or ""
    endpoint = urlsplit(request_url).path or "unknown"
    logging.info(
        "Meta quota endpoint=%s status=%s utilization=%.1f%% tier=%s headers=%s",
        endpoint,
        getattr(response, "status_code", "unknown"),
        snapshot.utilization_pct,
        snapshot.access_tier or "unknown",
        ",".join(snapshot.header_names),
    )

    status_code = int(getattr(response, "status_code", 0) or 0)
    threshold = max(1, min(100, _get_int_env("FB_RATE_LIMIT_PACE_THRESHOLD", 90)))
    max_delay = max(0, _get_int_env("FB_RATE_LIMIT_MAX_PACE_SECONDS", 30))
    if (
        status_code < 400
        and max_delay > 0
        and snapshot.utilization_pct >= threshold
    ):
        if snapshot.reset_seconds:
            delay = min(snapshot.reset_seconds, max_delay)
        else:
            delay = min(
                max_delay,
                max(1, int((snapshot.utilization_pct - threshold + 1) * 2)),
            )
        logging.warning(
            "Meta quota utilization is %.1f%%; pacing for %ds before the next call",
            snapshot.utilization_pct,
            delay,
        )
        time.sleep(delay)

    return response
