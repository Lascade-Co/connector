"""Facebook ads source helpers"""

import functools
import itertools
import time
from typing import Any, Iterator, Sequence, Dict, Optional

import dlt
import humanize
import pendulum
from dlt.common import logger
from dlt.common.configuration.inject import with_config
from dlt.common.time import ensure_pendulum_datetime_utc
from dlt.common.typing import DictStrAny, TDataItem, TDataItems
from dlt.sources.helpers import requests
from dlt.sources.helpers.requests import Client
from facebook_business import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookResponse

from .exceptions import InsightsJobFailed, InsightsJobTimeout
from .rate_limit import RATE_LIMIT_CODES, observe_meta_response
from .settings import (
    FACEBOOK_INSIGHTS_RETENTION_PERIOD,
    INSIGHTS_PRIMARY_KEY,
    TFbMethod,
)
from .utils import AbstractCrudObject, AbstractObject


def get_start_date(
    incremental_start_date: dlt.sources.incremental[str],
    attribution_window_days_lag: int = 7,
) -> pendulum.DateTime:
    """
    Get the start date for incremental loading of Facebook Insights data.
    """
    start_date: pendulum.DateTime = ensure_pendulum_datetime_utc(
        incremental_start_date.start_value
    ).subtract(days=attribution_window_days_lag)

    # facebook forgets insights so trim the lag and warn
    min_start_date = pendulum.today().subtract(
        months=FACEBOOK_INSIGHTS_RETENTION_PERIOD
    )
    if start_date < min_start_date:
        logger.warning(
            "%s: Start date is earlier than %s months ago, using %s instead. "
            "For more information, see https://www.facebook.com/business/help/1695754927158071?id=354406972049255",
            "facebook_insights",
            FACEBOOK_INSIGHTS_RETENTION_PERIOD,
            min_start_date,
        )
        start_date = min_start_date
        incremental_start_date.start_value = min_start_date.to_date_string()

    # lag the incremental start date by attribution window lag
    incremental_start_date.start_value = start_date.to_date_string()
    return start_date


def process_report_item(item: AbstractObject) -> DictStrAny:
    d: DictStrAny = item.export_all_data()
    for pki in INSIGHTS_PRIMARY_KEY:
        if pki not in d:
            d[pki] = "no_" + pki

    return d


# ---------------------------------------------------------------------------
# INSIGHTS FLATTENING
# ---------------------------------------------------------------------------
from .settings import (
    SELECTED_ACTION_TYPES,
    SELECTED_ACTION_VALUE_TYPES,
    SELECTED_WEBSITE_CTR_TYPES,
    SELECTED_CPA_TYPES,
    SELECTED_PURCHASE_ROAS_TYPES,
    ACTIONS_PREFIX,
    ACTION_VALUES_PREFIX,
    CPA_PREFIX,
)


def _first_numeric(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


_TRIAL_EVENT_ALIASES = {
    "omni": ("omni_start_trial",),
    "components": (
        (
            "start_trial_mobile_app",
            "app_custom_event.fb_mobile_start_trial",
            "fb_mobile_start_trial",
        ),
        ("offsite_conversion.fb_pixel_start_trial",),
    ),
    "fallback": ("start_trial",),
}

_SUBSCRIPTION_EVENT_ALIASES = {
    "omni": ("omni_subscribe",),
    "components": (
        (
            "subscribe_mobile_app",
            "app_custom_event.fb_mobile_subscribe",
            "fb_mobile_subscribe",
        ),
        ("offsite_conversion.fb_pixel_subscribe",),
    ),
    "fallback": ("subscribe",),
}


def _action_values_by_type(
    item: DictStrAny, field_names: Sequence[str]
) -> dict[str, float]:
    """Merge arrays by action type without counting the same type twice."""

    values: dict[str, float] = {}
    for field_name in field_names:
        entries = item.get(field_name)
        if not isinstance(entries, list):
            continue
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            action_type = str(entry.get("action_type") or "").strip().lower()
            value = _first_numeric(entry.get("value"))
            if action_type and value is not None:
                values.setdefault(action_type, value)
    return values


def _event_metric(
    item: DictStrAny,
    field_names: Sequence[str],
    aliases: dict[str, tuple],
) -> float:
    """Prefer Meta's omni rollup; otherwise combine one web and one app value."""

    values = _action_values_by_type(item, field_names)
    for action_type in aliases["omni"]:
        if action_type in values:
            return values[action_type]

    component_total = 0.0
    found_component = False
    for alias_group in aliases["components"]:
        for action_type in alias_group:
            if action_type in values:
                component_total += values[action_type]
                found_component = True
                break
    if found_component:
        return component_total

    for action_type in aliases["fallback"]:
        if action_type in values:
            return values[action_type]
    return 0.0


def _add_subscription_event_metrics(item: DictStrAny) -> None:
    """Add stable ad-attributed trial/subscription count and value columns."""

    item["trial_starts"] = _event_metric(
        item, ("conversions", "actions"), _TRIAL_EVENT_ALIASES
    )
    item["trial_start_value"] = _event_metric(
        item, ("conversion_values", "action_values"), _TRIAL_EVENT_ALIASES
    )
    item["subscriptions"] = _event_metric(
        item, ("conversions", "actions"), _SUBSCRIPTION_EVENT_ALIASES
    )
    item["subscription_value"] = _event_metric(
        item,
        ("conversion_values", "action_values"),
        _SUBSCRIPTION_EVENT_ALIASES,
    )


def _expand_action_list(
    item: DictStrAny,
    field_name: str,
    selected: Sequence[str],
    prefix: str,
    action_key: str = "action_type",
    value_key: str = "value",
) -> Sequence[str]:
    """Expand a list-of-dicts action field into scalar columns for selected types.

    Example input: item["actions"] = [{"action_type": "link_click", "value": "50"}, ...]
    Resulting keys: actions_link_click=50.0
    """
    src = item.get(field_name)
    if not isinstance(src, list):
        return []

    # Capture only the first occurrence for exact action_type matches
    seen: Dict[str, float] = {}
    selected_set = set(selected)
    for e in src:
        if not isinstance(e, dict):
            continue
        a = e.get(action_key)
        if a not in selected_set or a in seen:
            continue
        num = _first_numeric(e.get(value_key))
        if num is None:
            continue
        seen[a] = num

    created: list[str] = []
    for a, num in seen.items():
        key = f"{prefix}{a}"
        # do not overwrite if already present
        if key not in item:
            item[key] = num
        created.append(key)

    return created


def _flatten_values_series(
    item: DictStrAny, field_name: str, out_key: str = None
) -> Optional[str]:
    """Flatten FB 'values' series fields into a single scalar using the first value.

    Example shape:
    [{"indicator": "...", "values": [{"value": "0.97", "attribution_windows": ["default"]}]}]
    """
    data = item.get(field_name)
    if not (isinstance(data, list) and data):
        return None
    first = data[0]
    values = first.get("values") if isinstance(first, dict) else None
    if isinstance(values, list) and values:
        val = values[0].get("value") if isinstance(values[0], dict) else None
        num = _first_numeric(val)
        if num is not None:
            target = out_key or field_name
            item[target] = num
            # only remove the source if target differs
            if target != field_name:
                item.pop(field_name, None)
            logger.info(
                "flatten_facebook_insights: %s -> %s=%s", field_name, target, num
            )
            return target
    return None


def flatten_facebook_insights(item: DictStrAny) -> DictStrAny:
    """Map complex FB insights fields to flat scalar columns.

    Only selected action types are expanded to avoid excessive schema growth.
    """
    _add_subscription_event_metrics(item)

    # Action-type lists
    _expand_action_list(item, "actions", SELECTED_ACTION_TYPES, ACTIONS_PREFIX)
    _expand_action_list(
        item, "action_values", SELECTED_ACTION_VALUE_TYPES, ACTION_VALUES_PREFIX
    )
    _expand_action_list(item, "cost_per_action_type", SELECTED_CPA_TYPES, CPA_PREFIX)
    _expand_action_list(item, "website_ctr", SELECTED_WEBSITE_CTR_TYPES, "website_ctr_")
    _expand_action_list(
        item, "purchase_roas", SELECTED_PURCHASE_ROAS_TYPES, "purchase_roas_"
    )

    # Other complex fields
    _flatten_values_series(item, "cost_per_result")  # keep same key as scalar
    return item


def get_data_chunked(
    method: TFbMethod,
    fields: Sequence[str],
    states: Sequence[str],
    chunk_size: int,
    extra_params: Optional[DictStrAny] = None,
) -> Iterator[TDataItems]:
    # add pagination and chunk into lists
    params: DictStrAny = {"limit": chunk_size}
    if states:
        params.update({"effective_status": states})
    if extra_params:
        params.update(extra_params)
    it: map[DictStrAny] = map(
        lambda c: c.export_all_data(), method(fields=fields, params=params)
    )
    while True:
        chunk = list(itertools.islice(it, chunk_size))
        if not chunk:
            break
        yield chunk


def enrich_ad_objects(fb_obj_type: AbstractObject, fields: Sequence[str]) -> Any:
    """Returns a transformation that will enrich any of the resources returned by `` with additional fields

    In example below we add "thumbnail_url" to all objects loaded by `ad_creatives` resource:
    >>> fb_ads = facebook_ads_source()
    >>> fb_ads.ad_creatives.add_step(enrich_ad_objects(AdCreative, ["thumbnail_url"]))

    Internally, the method uses batch API to get data efficiently. Refer to demo script for full examples

    Args:
        fb_obj_type (AbstractObject): A Facebook Business object type (Ad, Campaign, AdSet, AdCreative, Lead). Import those types from this module
        fields (Sequence[str]): A list/tuple of fields to add to each object.

    Returns:
        ItemTransformFunctionWithMeta[TDataItems]: A transformation function to be added to a resource with `add_step` method
    """

    def _wrap(items: TDataItems, meta: Any = None) -> TDataItems:
        api_batch = FacebookAdsApi.get_default_api().new_batch()

        def update_item(resp: FacebookResponse, item: TDataItem) -> None:
            item.update(resp.json())

        def fail(resp: FacebookResponse) -> None:
            raise resp.error()

        for item in items:
            o: AbstractCrudObject = fb_obj_type(item["id"])
            o.api_get(
                fields=fields,
                batch=api_batch,
                success=functools.partial(update_item, item=item),
                failure=fail,
            )
        api_batch.execute()
        return items

    return _wrap


JOB_TIMEOUT_INFO = """This is an intermittent error and may resolve itself on subsequent queries to the Facebook API.
You should remove the fields in `fields` argument that are not necessary, as that may help improve the reliability of the Facebook API."""


def execute_job(
    job: AbstractCrudObject,
    insights_max_wait_to_start_seconds: int = 5 * 60,
    insights_max_wait_to_finish_seconds: int = 60 * 60,
    insights_max_async_sleep_seconds: int = 60,
) -> AbstractCrudObject:
    time_start = time.time()
    sleep_time = 10
    while True:
        duration = time.time() - time_start
        job = job.api_get()
        status = job.get("async_status")
        percent_complete = int(job.get("async_percent_completion") or 0)

        job_id = job.get("id", "unknown")
        logger.info("%s, %d%% done", status, percent_complete)

        if status == "Job Completed" and percent_complete >= 100:
            return job

        if status in ("Job Failed", "Job Skipped"):
            details = job.export_all_data()
            error_message = (
                details.get("error_user_msg")
                or details.get("error_message")
                or "Meta did not include an error message"
            )
            try:
                error_code = int(details["error_code"])
            except (KeyError, TypeError, ValueError):
                error_code = None
            try:
                error_subcode = int(details["error_subcode"])
            except (KeyError, TypeError, ValueError):
                error_subcode = None
            raise InsightsJobFailed(
                "facebook_insights",
                f"Insights job {job_id} ended with {status}: {error_message} "
                f"(code={error_code}, subcode={error_subcode})",
                status=status,
                error_code=error_code,
                error_subcode=error_subcode,
            )

        if duration > insights_max_wait_to_start_seconds and percent_complete == 0:
            pretty_error_message = (
                "Insights job {} did not start after {} seconds. " + JOB_TIMEOUT_INFO
            )
            raise InsightsJobTimeout(
                "facebook_insights",
                pretty_error_message.format(job_id, insights_max_wait_to_start_seconds),
            )
        elif duration > insights_max_wait_to_finish_seconds:
            pretty_error_message = (
                "Insights job {} did not complete after {} seconds. " + JOB_TIMEOUT_INFO
            )
            raise InsightsJobTimeout(
                "facebook_insights",
                pretty_error_message.format(
                    job_id, insights_max_wait_to_finish_seconds
                ),
            )

        logger.info("sleeping for %d seconds until job is done", sleep_time)
        time.sleep(sleep_time)
        sleep_time = min(insights_max_async_sleep_seconds, 2 * sleep_time)


@functools.lru_cache(maxsize=128)
def get_ads_account(
    account_id: str, access_token: str, request_timeout: float, app_api_version: str
) -> AdAccount:
    """Create one explicitly API-bound account object per process/configuration."""

    notify_on_token_expiration()

    # Only true transient backend errors are retried automatically. Quota
    # failures must surface immediately so the account can be parked.
    retryable_fb_codes = frozenset((1, 2, 341))

    def retry_on_limit(response: requests.Response, exception: BaseException) -> bool:
        # dlt invokes the predicate with response=None for exception-only
        # failures (e.g. connection errors). Those are handled by the Client's
        # exception-retry path, so abstain here.
        if response is None:
            return False

        # Parse the JSON error body if present; FB returns one for all
        # well-formed errors.
        error: Dict[str, Any] = {}
        try:
            error = response.json().get("error") or {}
        except Exception:
            error = {}

        code = error.get("code")
        message = error.get("message") or ""
        is_5xx = 500 <= response.status_code < 600

        if response.status_code == 429 or code in RATE_LIMIT_CODES:
            logger.warning(
                "facebook_ads source is quota-limited by Meta (code %s); "
                "stopping automatic HTTP retries",
                code,
            )
            return False

        # "Please reduce the amount of data..." is deterministic — retrying the
        # same request won't help. Surface it immediately so the caller can
        # shrink the page size / fields and retry adaptively.
        if code == 1 and "reduce the amount of data" in message.lower():
            return False

        # Transient gateway errors with no parseable FB error code: still retry
        # at the status level (preserves resilience for 502/503/timeouts that
        # don't include a JSON error body).
        if not error and is_5xx:
            return True

        should_retry = code in retryable_fb_codes
        if should_retry:
            logger.warning(
                "facebook_ads source will retry due to %s with error code %s",
                message,
                code,
            )
        return should_retry

    retry_session = Client(
        request_timeout=request_timeout,
        raise_for_status=False,
        # The predicate above is the sole arbiter for status-based retries: it
        # explicitly handles 5xx (with or without an FB error body) and
        # explicitly opts out of the deterministic "reduce data" case. Keeping
        # 5xx in status_codes here would force 12 backoff attempts on that
        # deterministic error before the ad_creatives caller can shrink the
        # page size, since dlt's Client OR's status retries with the predicate.
        status_codes=(),
        retry_condition=retry_on_limit,
        request_max_attempts=4,
        request_backoff_factor=2,
    ).session
    retry_session.hooks.setdefault("response", []).append(observe_meta_response)
    retry_session.params.update({"access_token": access_token})  # type: ignore
    # patch dlt requests session with retries
    API = FacebookAdsApi.init(
        account_id="act_" + account_id,
        access_token=access_token,
        api_version=app_api_version,
    )
    API._session.requests = retry_session
    return AdAccount(fbid="act_" + account_id, api=API)


@with_config(sections=("sources", "facebook_ads"))
def notify_on_token_expiration(access_token_expires_at: int = None) -> None:
    """Notifies (currently via logger) if access token expires in less than 7 days. Needs `access_token_expires_at` to be configured."""
    if not access_token_expires_at:
        logger.warning(
            "Token expiration time notification disabled. Configure token expiration timestamp in access_token_expires_at config value"
        )
    else:
        expires_at = pendulum.from_timestamp(access_token_expires_at)
        if expires_at < pendulum.now().add(days=7):
            logger.error(
                f"Access Token expires in {humanize.precisedelta(pendulum.now() - expires_at)}. Replace the token now!"
            )
