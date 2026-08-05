"""Shared execution helpers for independently checkpointed Facebook loads."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from typing import Any

import dlt

from facebook_ads.insights import plan_insights_date_windows
from pipelines.facebook.creative_status import (
    account_has_partial_resources,
    format_partial_resources,
    get_partial_resources,
)


DEFAULT_INITIAL_LOAD_DAYS = 30
DEFAULT_ATTRIBUTION_LAG_DAYS = 7
DEFAULT_LOAD_WINDOW_DAYS = 8
DEFAULT_CLICKHOUSE_TIMEOUT_SECONDS = 900


def positive_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except ValueError:
        logging.warning("Invalid %s=%r; defaulting to %d", name, raw, default)
        return default
    if value < 1:
        logging.warning("Invalid %s=%r; defaulting to %d", name, raw, default)
        return default
    return value


def clickhouse_destination(destination_name: str | None = None):
    """Preserve configured credentials while extending long merge reads."""

    timeout = positive_int_env(
        "FB_CLICKHOUSE_SEND_RECEIVE_TIMEOUT_SECONDS",
        DEFAULT_CLICKHOUSE_TIMEOUT_SECONDS,
    )
    return dlt.destinations.clickhouse(
        destination_name=destination_name,
        credentials={"send_receive_timeout": timeout},
    )


def run_structural_resources(
    pipeline: Any,
    resources: list[Callable[..., Any]],
    creds: list[dict[str, str]],
    group_name: str,
) -> None:
    """Checkpoint each current-state resource and stop an account after throttling."""

    account_id = creds[0]["account_id"]
    for source in resources:
        resource = source(creds, group_name)
        pipeline.run(resource)
        if account_has_partial_resources(account_id):
            logging.warning(
                "Facebook account %s has a quota-limited resource; skipping its "
                "remaining current-state resources for this run",
                account_id,
            )
            break


def raise_for_partial_resources(pipeline_label: str) -> None:
    """Fail visibly after successful packages have committed valid merge rows/state."""

    if not get_partial_resources():
        return
    raise RuntimeError(
        f"{pipeline_label} resources were only partially loaded "
        f"({format_partial_resources()}). Valid rows and checkpoints were committed; "
        "no saved rows were corrupted. A later reconciliation will retry the "
        "missing subset."
    )


def initial_load_days(backfill_env_name: str) -> int:
    return positive_int_env(backfill_env_name, DEFAULT_INITIAL_LOAD_DAYS)


def run_insights_in_windows(
    pipeline: Any,
    insights_resource: Callable[..., Any],
    creds: list[dict[str, str]],
    group_name: str,
    *,
    backfill_env_name: str,
) -> None:
    """Load each planned Insights window as its own durable dlt package."""

    account_id = creds[0]["account_id"]
    pipeline_state = pipeline.state
    if os.getenv(backfill_env_name) and not os.getenv("PIPELINE_NAME_SUFFIX"):
        # Local/manual backfills without the workflow's unique suffix must honor
        # the requested history instead of inheriting the daily cursor.
        pipeline_state = {}
    windows = plan_insights_date_windows(
        pipeline_state,
        account_id,
        initial_load_past_days=initial_load_days(backfill_env_name),
        attribution_window_days_lag=DEFAULT_ATTRIBUTION_LAG_DAYS,
        max_days=positive_int_env(
            "FB_INSIGHTS_LOAD_WINDOW_DAYS", DEFAULT_LOAD_WINDOW_DAYS
        ),
    )
    for window_start, window_end in windows:
        start = window_start.to_date_string()
        end = window_end.to_date_string()
        logging.info(
            "Loading Facebook Insights account=%s window=%s..%s",
            account_id,
            start,
            end,
        )
        pipeline.run(
            insights_resource(
                creds,
                group_name,
                report_start_date=start,
                report_end_date=end,
            )
        )
