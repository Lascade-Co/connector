"""Loads campaigns, ads sets, ads, leads and insight data from Facebook Marketing API"""

from typing import Any, Iterator, Sequence

import dlt
import logging
import os
from dlt.common import pendulum
from dlt.common.typing import TDataItems
from dlt.sources import DltResource

from .creatives import iter_creatives
from .helpers import flatten_facebook_insights
from .helpers import (
    get_data_chunked,
    enrich_ad_objects,
    get_start_date,
    process_report_item,
    execute_job,
    get_ads_account,
)
from .exceptions import InsightsJobFailed
from .insights import (
    INSIGHTS_WINDOW_CHECKPOINT,
    iter_date_windows,
    merge_report_rows,
    split_insight_fields,
)
from .rate_limit import RATE_LIMIT_CODES
from .settings import (
    DEFAULT_AD_FIELDS,
    DEFAULT_ADCREATIVE_FIELDS,
    DEFAULT_ADSET_FIELDS,
    DEFAULT_CAMPAIGN_FIELDS,
    DEFAULT_LEAD_FIELDS,
    TFbMethod,
    TInsightsBreakdownOptions,
)
from .settings import (
    FACEBOOK_INSIGHTS_RETENTION_PERIOD,
    ALL_ACTION_BREAKDOWNS,
    ALL_ACTION_ATTRIBUTION_WINDOWS,
    DEFAULT_INSIGHT_FIELDS,
    INSIGHT_FIELDS_TYPES,
    INSIGHTS_PRIMARY_KEY,
    INSIGHTS_BREAKDOWNS_OPTIONS,
    INVALID_INSIGHTS_FIELDS,
    TInsightsLevels,
)
from .utils import (
    Ad,
)
from .utils import debug_access_token, get_long_lived_token


_ACTION_INSIGHT_FIELDS = frozenset(
    (
        "actions",
        "action_values",
        "conversions",
        "conversion_values",
        "website_ctr",
        "cost_per_action_type",
        "cost_per_result",
        "purchase_roas",
    )
)
_SPLITTABLE_INSIGHTS_ERROR_SUBCODES = frozenset((1487534,))


def _is_splittable_insights_failure(exc: InsightsJobFailed) -> bool:
    """Return true only for terminal failures that smaller ranges can repair."""

    if exc.error_subcode in _SPLITTABLE_INSIGHTS_ERROR_SUBCODES:
        return True
    message = str(exc).lower()
    return exc.error_code == 1 and any(
        marker in message
        for marker in ("reduce the amount of data", "too much data", "query too large")
    )


def _get_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logging.warning("Invalid %s=%r; defaulting to %d", name, raw, default)
        return default


@dlt.source(name="facebook_ads")
def facebook_ads_source(
    account_id: str = dlt.config.value,
    access_token: str = dlt.secrets.value,
    chunk_size: int = 200,
    request_timeout: float = 300.0,
    app_api_version: str = None,
) -> Sequence[DltResource]:
    """Returns a list of resources to load campaigns, ad sets, ads, creatives and ad leads data from Facebook Marketing API.

    All the resources have `replace` write disposition by default and define primary keys. Resources are parametrized and allow the user
    to change the set of fields that will be loaded from the API and the object statuses that will be loaded. See the demonstration script for details.

    You can convert the source into merge resource to keep the deleted objects. Currently, Marketing API does not return deleted objects. See the demo script.

    We also provide a transformation `enrich_ad_objects` that you can add to any of the resources to get additional data per object via `object.get_api`

    Args:
        account_id (str, optional): Account id associated with add manager. See README.md
        access_token (str, optional): Access token associated with the Business Facebook App. See README.md
        chunk_size (int, optional): A size of the page and batch request. You may need to decrease it if you request a lot of fields. Defaults to 200.
        request_timeout (float, optional): Connection timeout. Defaults to 300.0.
        app_api_version(str, optional): A version of the facebook api required by the app for which the access tokens were issued ie. 'v17.0'. Defaults to the facebook_business library default version

    Returns:
        Sequence[DltResource]: campaigns, ads, ad_sets, ad_creatives, leads
    """
    account = get_ads_account(
        account_id, access_token, request_timeout, app_api_version
    )

    @dlt.resource(primary_key="id", write_disposition="replace")
    def campaigns(
        fields: Sequence[str] = DEFAULT_CAMPAIGN_FIELDS, states: Sequence[str] = None
    ) -> Iterator[TDataItems]:
        yield get_data_chunked(account.get_campaigns, fields, states, chunk_size)

    @dlt.resource(primary_key="id", write_disposition="replace")
    def ads(
        fields: Sequence[str] = DEFAULT_AD_FIELDS, states: Sequence[str] = None
    ) -> Iterator[TDataItems]:
        yield get_data_chunked(account.get_ads, fields, states, chunk_size)

    @dlt.resource(primary_key="id", write_disposition="replace")
    def ad_sets(
        fields: Sequence[str] = DEFAULT_ADSET_FIELDS, states: Sequence[str] = None
    ) -> Iterator[TDataItems]:
        yield get_data_chunked(account.get_ad_sets, fields, states, chunk_size)

    @dlt.transformer(primary_key="id", write_disposition="replace", selected=True)
    def leads(
        items: TDataItems,
        fields: Sequence[str] = DEFAULT_LEAD_FIELDS,
        states: Sequence[str] = None,
    ) -> Iterator[TDataItems]:
        for item in items:
            ad = Ad(item["id"])
            yield get_data_chunked(ad.get_leads, fields, states, chunk_size)

    @dlt.resource(primary_key="id", write_disposition="replace")
    def ad_creatives(
        fields: Sequence[str] = DEFAULT_ADCREATIVE_FIELDS, states: Sequence[str] = None
    ) -> Iterator[TDataItems]:
        yield from iter_creatives(account, account_id, fields, states)

    return campaigns, ads, ad_sets, ad_creatives, ads | leads


@dlt.source(name="facebook_ads")
def facebook_insights_source(
    account_id: str = dlt.config.value,
    access_token: str = dlt.secrets.value,
    initial_load_past_days: int = 30,
    fields: Sequence[str] = DEFAULT_INSIGHT_FIELDS,
    attribution_window_days_lag: int = 7,
    time_increment_days: int = 1,
    breakdowns: TInsightsBreakdownOptions = "ads_insights",
    action_breakdowns: Sequence[str] = ALL_ACTION_BREAKDOWNS,
    level: TInsightsLevels = "ad",
    action_attribution_windows: Sequence[str] = ALL_ACTION_ATTRIBUTION_WINDOWS,
    batch_size: int = 500,
    result_page_size: int = 100,
    request_timeout: int = 300,
    app_api_version: str = None,
    report_start_date: str = None,
    report_end_date: str = None,
    pre_flatten: Any = None,
) -> DltResource:
    """Incrementally loads insight reports with defined granularity level, fields, breakdowns etc.

    Reports use bounded multi-day request windows while retaining daily rows.
    On subsequent runs, the attribution lag is refreshed and typically fits in
    one request window.

    Core and unique-reach metrics may use separate async jobs when required by
    Meta field compatibility.

    Args:
        account_id: str = dlt.config.value,
        access_token: str = dlt.secrets.value,
        initial_load_past_days (int, optional): How many past days (starting from today) to initially load. Defaults to 30.
        fields (Sequence[str], optional): A list of fields to include in each report. Note that `breakdowns` option adds fields automatically. Defaults to DEFAULT_INSIGHT_FIELDS.
        attribution_window_days_lag (int, optional): Attribution window in days. The reports in attribution window are refreshed on each run. Defaults to 7.
        time_increment_days (int, optional): The report aggregation window in days. use 7 for weekly aggregation. Defaults to 1.
        breakdowns (TInsightsBreakdownOptions, optional): A presents with common aggregations. See settings.py for details. Defaults to "ads_insights_age_and_gender".
        action_breakdowns (Sequence[str], optional): Action aggregation types. See settings.py for details. Defaults to ALL_ACTION_BREAKDOWNS.
        level (TInsightsLevels, optional): The granularity level. Defaults to "ad".
        action_attribution_windows (Sequence[str], optional): Attribution windows for actions. Defaults to ALL_ACTION_ATTRIBUTION_WINDOWS.
        batch_size (int, optional): Page size submitted with the async report job. Defaults to 500.
        result_page_size (int, optional): Page size used to download a completed report. Defaults to 100.
        request_timeout (int, optional): Connection timeout. Defaults to 300.
        app_api_version(str, optional): A version of the facebook api required by the app for which the access tokens were issued ie. 'v17.0'. Defaults to the facebook_business library default version
        report_start_date (str, optional): Inclusive lower bound for one independently loaded report window.
        report_end_date (str, optional): Inclusive upper bound for one independently loaded report window.
        pre_flatten (Callable[[DictStrAny], DictStrAny], optional): Applied to each raw report row *before* flattening, so scalars derived from nested arrays inherit the transform. Used for currency normalization.

    Returns:
        DltResource: facebook_insights

    """
    account = get_ads_account(
        account_id, access_token, request_timeout, app_api_version
    )

    insights_max_wait_to_start_seconds = _get_int_env(
        "FB_INSIGHTS_MAX_WAIT_TO_START_SECONDS", 5 * 60
    )
    insights_max_wait_to_finish_seconds = _get_int_env(
        "FB_INSIGHTS_MAX_WAIT_TO_FINISH_SECONDS", 60 * 60
    )
    insights_max_async_sleep_seconds = _get_int_env(
        "FB_INSIGHTS_MAX_ASYNC_SLEEP_SECONDS", 60
    )
    insights_window_days = max(1, _get_int_env("FB_INSIGHTS_WINDOW_DAYS", 8))
    result_page_size = max(
        1, _get_int_env("FB_INSIGHTS_RESULT_PAGE_SIZE", result_page_size)
    )

    if (report_start_date is None) != (report_end_date is None):
        raise ValueError(
            "report_start_date and report_end_date must either both be set or both be omitted"
        )

    # we load with a defined lag
    initial_load_start_date = pendulum.today().subtract(days=initial_load_past_days)
    initial_load_start_date_str = initial_load_start_date.to_date_string()

    @dlt.resource(
        name=f"facebook_insights_{account_id}",
        primary_key=INSIGHTS_PRIMARY_KEY,
        write_disposition="merge",
        columns=INSIGHT_FIELDS_TYPES,
    )
    def facebook_insights(
        date_start: dlt.sources.incremental[str] = dlt.sources.incremental(
            "date_start", initial_value=initial_load_start_date_str
        )
    ) -> Iterator[TDataItems]:
        start_date = get_start_date(date_start, attribution_window_days_lag)
        end_date = pendulum.now()
        if report_start_date is not None:
            start_date = pendulum.parse(report_start_date)
            end_date = pendulum.parse(report_end_date)
            retention_start = pendulum.today().subtract(
                months=FACEBOOK_INSIGHTS_RETENTION_PERIOD
            )
            start_date = max(start_date, retention_start)
            if start_date > end_date:
                raise ValueError(
                    f"Facebook Insights report start {start_date} is after end {end_date}"
                )
            # Keep dlt's incremental filter aligned with the explicit API range.
            # Its persisted last_value still advances monotonically after load.
            date_start.start_value = start_date.to_date_string()

        requested_fields = list(
            set(fields)
            .union(INSIGHTS_BREAKDOWNS_OPTIONS[breakdowns]["fields"])
            .difference(INVALID_INSIGHTS_FIELDS)
        )
        core_fields, unique_fields = split_insight_fields(requested_fields)

        def fetch_report(
            report_fields: Sequence[str],
            report_start: pendulum.DateTime,
            report_end: pendulum.DateTime,
        ) -> list[dict]:
            query = {
                "level": level,
                # "action_breakdowns": list(action_breakdowns),
                # "breakdowns": list(
                #     INSIGHTS_BREAKDOWNS_OPTIONS[breakdowns]["breakdowns"]
                # ),
                "limit": batch_size,
                "fields": list(report_fields),
                "time_increment": time_increment_days,
                "time_range": {
                    "since": report_start.to_date_string(),
                    "until": report_end.to_date_string(),
                },
            }
            if _ACTION_INSIGHT_FIELDS.intersection(report_fields):
                query["action_attribution_windows"] = list(action_attribution_windows)
            try:
                job = execute_job(
                    account.get_insights(params=query, is_async=True),
                    insights_max_wait_to_start_seconds=insights_max_wait_to_start_seconds,
                    insights_max_wait_to_finish_seconds=insights_max_wait_to_finish_seconds,
                    insights_max_async_sleep_seconds=insights_max_async_sleep_seconds,
                )
                return [
                    process_report_item(item)
                    for item in job.get_result(params={"limit": result_page_size})
                ]
            except InsightsJobFailed as exc:
                if exc.error_code in RATE_LIMIT_CODES:
                    raise
                if not _is_splittable_insights_failure(exc):
                    raise
                if report_start >= report_end:
                    raise
                days = (report_end.date() - report_start.date()).days + 1
                left_days = max(1, days // 2)
                left_end = report_start.add(days=left_days - 1)
                right_start = left_end.add(days=1)
                logging.warning(
                    "Meta Insights range %s..%s failed; splitting into smaller ranges",
                    report_start.to_date_string(),
                    report_end.to_date_string(),
                )
                return fetch_report(
                    report_fields, report_start, left_end
                ) + fetch_report(report_fields, right_start, report_end)

        windows = (
            ((start_date, end_date),)
            if report_start_date is not None
            else iter_date_windows(start_date, end_date, insights_window_days)
        )
        for window_start, window_end in windows:
            report_groups = [fetch_report(core_fields, window_start, window_end)]
            if unique_fields:
                report_groups.append(
                    fetch_report(unique_fields, window_start, window_end)
                )
            yield merge_report_rows(report_groups)
            dlt.current.resource_state()[
                INSIGHTS_WINDOW_CHECKPOINT
            ] = window_end.to_date_string()

    # Any pre-flatten transform must run first so that the scalar columns
    # flattening derives from the nested arrays inherit it.
    if pre_flatten is not None:
        facebook_insights.add_map(pre_flatten, insert_at=1)
        return facebook_insights.add_map(flatten_facebook_insights, insert_at=2)

    # Attach a lightweight map to flatten complex array/object fields to scalar columns
    return facebook_insights.add_map(flatten_facebook_insights, insert_at=1)
