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
    # Not a metric but a property of the row's account. It must appear in every
    # report so a row returned by only one of the split jobs still states its
    # reporting currency; see pipelines/facebook/currency.py.
    "account_currency",
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


def _is_zero(value: Any) -> bool:
    """True only when the value is present and parses to exactly 0."""

    try:
        return float(value) == 0.0
    except (TypeError, ValueError):
        return False


def _is_positive(value: Any) -> bool:
    try:
        return float(value) > 0.0
    except (TypeError, ValueError):
        return False


# Each count is paired only with the ratios that divide by it, so a positive
# ratio contradicts a zeroed count. Pairing anything wider fails normal windows:
# an ad that served impressions and got no clicks legitimately reports
# ``clicks=0`` with ``cpm>0``, which is why ``cpm`` sits with ``impressions``
# alone. Checked against 160,599 production rows: no row matches a pair except
# the known-degraded ones.
_COUNT_RATIO_PAIRS = (
    # impressions: ctr = clicks/impressions, cpm = spend/impressions
    ("impressions", ("ctr", "cpc", "cpm")),
    # clicks: ctr = clicks/impressions, cpc = spend/clicks
    ("clicks", ("ctr", "cpc")),
    # spend: numerator of every per-cost ratio
    ("spend", ("cpc", "cpm", "cpp")),
    # reach and unique_clicks come from the separate unique-metrics report
    ("reach", ("frequency", "cpp", "unique_ctr")),
    ("unique_clicks", ("unique_ctr",)),
)


def degraded_report_rows(rows: Iterable[DictStrAny]) -> list[DictStrAny]:
    """Rows whose ratio metrics prove delivery that the absolute counts deny.

    Meta occasionally returns a completed report with its absolute delivery
    counts zeroed while the derived ratios stay correct. The ratios then prove
    delivery the counts deny, so the report is refused rather than merged over
    stored rows.

    For the integer counts the contradiction is exact. ``spend`` is rounded to
    minor units, so a sub-fils amount could in principle render as ``0`` beside
    a positive ratio; Meta derives the ratios from the same rounded value, and
    no production row has ever shown that shape.
    """

    return [
        row
        for row in rows
        if any(
            _is_zero(row.get(count))
            and any(_is_positive(row.get(ratio)) for ratio in ratios)
            for count, ratios in _COUNT_RATIO_PAIRS
        )
    ]


def unique_metrics_missing_despite_delivery(rows: Iterable[DictStrAny]) -> bool:
    """True when a window shows delivery but no row carries any unique metric.

    Only meaningful when the unique-metrics report was actually requested; the
    caller checks that. A production anomaly whose mechanism is not yet
    understood, so it only warns.
    """

    window = list(rows)
    if not any(_is_positive(row.get("impressions")) for row in window):
        return False
    return not any(
        row.get(field) is not None
        for row in window
        for field in UNIQUE_INSIGHT_METRIC_FIELDS
    )
