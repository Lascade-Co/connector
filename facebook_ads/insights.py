"""Planning and merge helpers for bounded Meta Insights reports."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
import logging
from typing import Any

import pendulum
from dlt.common.time import ensure_pendulum_datetime_utc
from dlt.common.typing import DictStrAny

from .settings import FACEBOOK_INSIGHTS_RETENTION_PERIOD, INSIGHTS_PRIMARY_KEY


INSIGHTS_SOURCE_NAME = "facebook_ads"
INSIGHTS_WINDOW_CHECKPOINT = "last_completed_window_end"


INSIGHT_IDENTITY_FIELDS = (
    "account_id",
    "account_name",
    "campaign_id",
    "campaign_name",
    "adset_id",
    "adset_name",
    "ad_id",
    "ad_name",
    "date_start",
    "date_stop",
)

UNIQUE_INSIGHT_METRIC_FIELDS = frozenset(
    ("reach", "frequency", "unique_clicks", "unique_ctr", "cpp")
)


def split_insight_fields(fields: Sequence[str]) -> tuple[list[str], list[str]]:
    """Split expensive unique metrics while preserving row identity."""

    requested = set(fields)
    identity = requested.intersection(INSIGHT_IDENTITY_FIELDS)
    unique_metrics = requested.intersection(UNIQUE_INSIGHT_METRIC_FIELDS)
    core = requested.difference(UNIQUE_INSIGHT_METRIC_FIELDS).union(identity)
    unique = identity.union(unique_metrics)
    return sorted(core), sorted(unique) if unique_metrics else []


def iter_date_windows(
    start_date: pendulum.DateTime,
    end_date: pendulum.DateTime,
    max_days: int,
) -> Iterator[tuple[pendulum.DateTime, pendulum.DateTime]]:
    """Yield inclusive, non-overlapping report windows clamped to ``end_date``."""

    cursor = start_date
    while cursor <= end_date:
        window_end = min(cursor.add(days=max_days - 1), end_date)
        yield cursor, window_end
        cursor = window_end.add(days=1)


def _state_datetime(value: Any, label: str) -> pendulum.DateTime | None:
    if value in (None, ""):
        return None
    try:
        return ensure_pendulum_datetime_utc(value)
    except (TypeError, ValueError):
        logging.warning("Ignoring invalid Facebook Insights %s=%r", label, value)
        return None


def plan_insights_date_windows(
    pipeline_state: dict[str, Any],
    account_id: str,
    *,
    initial_load_past_days: int,
    attribution_window_days_lag: int,
    max_days: int,
    end_date: pendulum.DateTime | None = None,
) -> list[tuple[pendulum.DateTime, pendulum.DateTime]]:
    """Plan independently loadable windows from destination-restored state.

    The explicit completed-window checkpoint advances even when Meta returns no
    rows. Existing pipelines without that checkpoint fall back to dlt's
    incremental cursor, preserving their current progress during migration.
    """

    end_date = end_date or pendulum.today()
    resource_name = f"facebook_insights_{account_id}"
    resource_state = (
        pipeline_state.get("sources", {})
        .get(INSIGHTS_SOURCE_NAME, {})
        .get("resources", {})
        .get(resource_name, {})
    )
    checkpoint = _state_datetime(
        resource_state.get(INSIGHTS_WINDOW_CHECKPOINT),
        INSIGHTS_WINDOW_CHECKPOINT,
    )
    incremental_value = _state_datetime(
        resource_state.get("incremental", {}).get("date_start", {}).get("last_value"),
        "incremental.date_start.last_value",
    )
    completed_through = checkpoint or incremental_value

    if completed_through is None:
        start_date = end_date.subtract(days=max(1, initial_load_past_days))
    else:
        completed_through = min(completed_through, end_date)
        start_date = completed_through.subtract(
            days=max(0, attribution_window_days_lag)
        )

    retention_start = end_date.subtract(months=FACEBOOK_INSIGHTS_RETENTION_PERIOD)
    start_date = max(start_date, retention_start)
    return list(iter_date_windows(start_date, end_date, max(1, max_days)))


def report_row_key(item: DictStrAny) -> tuple[Any, ...]:
    return tuple(item[field] for field in INSIGHTS_PRIMARY_KEY)


def merge_report_rows(
    report_groups: Iterable[Iterable[DictStrAny]],
) -> list[DictStrAny]:
    """Outer-union core and unique report rows using the full destination key."""

    merged: dict[tuple[Any, ...], DictStrAny] = {}
    for group in report_groups:
        for item in group:
            key = report_row_key(item)
            merged.setdefault(key, {}).update(item)
    return list(merged.values())
