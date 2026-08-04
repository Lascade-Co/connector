"""Planning and merge helpers for bounded Meta Insights reports."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from typing import Any

import pendulum
from dlt.common.typing import DictStrAny

from .settings import INSIGHTS_PRIMARY_KEY


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
