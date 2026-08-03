"""Helpers for handling Meta Graph API ads-management rate limits.

When the API throttles us (codes 17, 32, 80000-80006, 613) it returns usage
headers describing how long until access is regained. We parse those to wait
exactly as long as needed instead of blindly backing off.
"""

import json
import logging
import time
from typing import Callable, Iterable, Mapping, Optional

from dlt.extract.exceptions import ResourceExtractionError
from facebook_business.exceptions import FacebookRequestError


# Meta error codes signaling app/account/user-level rate limiting that's
# worth waiting out (vs. permanent failures we should surface immediately).
# - 4:    Application request limit reached (app-level)
# - 17:   User request limit reached (user-level)
# - 32:   Page-level throttling
# - 613:  Generic per-app/per-account rate cap
# - 80000-80006, 80014: Marketing API business-use-case limits
RATE_LIMIT_CODES = frozenset(
    (4, 17, 32, 613, 80000, 80001, 80002, 80003, 80004, 80005, 80006, 80014)
)

# Hard cap on a single sleep — beyond this we'd rather skip the account and
# let the next scheduled run pick up via merge.
WAIT_CAP_SECONDS = 30 * 60

# Headers Meta uses to report usage; checked in priority order.
_USAGE_HEADERS = (
    "x-business-use-case-usage",
    "x-ad-account-usage",
    "x-app-usage",
)


def _flatten_usage_entries(payload: object) -> list:
    """Yield all dict entries in the usage payload across known shapes.

    `X-Business-Use-Case-Usage` nests by account id:
        `{"<id>": [{"estimated_time_to_regain_access": N, ...}]}`
    Other usage headers may be flat dicts. Include the payload itself when
    it's a dict so flat shapes are still scanned.
    """
    out: list = []
    if isinstance(payload, dict):
        out.append(payload)
        for v in payload.values():
            if isinstance(v, list):
                out.extend(e for e in v if isinstance(e, dict))
            elif isinstance(v, dict):
                out.append(v)
    elif isinstance(payload, list):
        out.extend(e for e in payload if isinstance(e, dict))
    return out


def _max_estimated_minutes(headers: Mapping[str, str]) -> Optional[int]:
    """Return the largest `estimated_time_to_regain_access` (minutes) found, or None."""
    best: Optional[int] = None
    for name in _USAGE_HEADERS:
        raw = headers.get(name) or headers.get(name.title()) or headers.get(name.upper())
        if not raw:
            continue
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError):
            continue
        for entry in _flatten_usage_entries(payload):
            minutes = entry.get("estimated_time_to_regain_access")
            if isinstance(minutes, (int, float)) and minutes > 0:
                best = max(best or 0, int(minutes))
    return best


def find_rate_limit_cause(exc: BaseException) -> Optional[FacebookRequestError]:
    """Walk the `__cause__` chain to find a rate-limit `FacebookRequestError`.

    dlt wraps non-dlt exceptions raised from a resource generator in
    `ResourceExtractionError(...) from ex`, so callers iterating a
    `DltResource` never see the original FB error directly. Returns the
    underlying `FacebookRequestError` if its code is in `RATE_LIMIT_CODES`,
    otherwise None.
    """
    cur: Optional[BaseException] = exc
    while cur is not None:
        if (
            isinstance(cur, FacebookRequestError)
            and cur.api_error_code() in RATE_LIMIT_CODES
        ):
            return cur
        cur = cur.__cause__
    return None


def parse_wait_seconds(
    headers: Optional[Mapping[str, str]],
    default: int,
) -> int:
    """Return how many seconds Meta says we need to wait, uncapped.

    Caller is responsible for comparing against any policy cap (e.g.
    `WAIT_CAP_SECONDS`) to decide whether to sleep-and-retry or skip the
    account. Falls back to `default` when no usage header is present or
    parseable.
    """
    minutes = _max_estimated_minutes(headers or {})
    seconds = minutes * 60 if minutes is not None else default
    return max(1, seconds)


def stream_with_rate_limit_retry(
    accounts: Iterable[dict],
    group_name: str,
    stream: Callable,
    *,
    resource_name: str,
    on_partial: Optional[Callable[[str], None]] = None,
    default_wait_seconds: int = 300,
):
    """Yield an account stream with one bounded Meta rate-limit retry.

    Non-rate-limit failures remain fatal. If Meta's wait exceeds the cap or a
    retry is throttled again, the affected account is skipped after notifying
    ``on_partial``. Callers that need a machine-detectable incomplete run can
    raise after their successful dlt loads have been committed.
    """
    for cred in accounts:
        account_id = cred["account_id"]
        try:
            yield from stream(cred, group_name)
            continue
        except (ResourceExtractionError, FacebookRequestError) as exc:
            facebook_error = find_rate_limit_cause(exc)
            if facebook_error is None:
                raise
            wait_seconds = parse_wait_seconds(
                facebook_error.http_headers(), default=default_wait_seconds
            )
            if wait_seconds > WAIT_CAP_SECONDS:
                logging.error(
                    "%s: account %s rate-limited (code %s) with estimated "
                    "wait %ds exceeding %ds cap; skipping. Already-yielded "
                    "items persist via merge.",
                    resource_name,
                    account_id,
                    facebook_error.api_error_code(),
                    wait_seconds,
                    WAIT_CAP_SECONDS,
                )
                if on_partial is not None:
                    on_partial(account_id)
                continue
            logging.warning(
                "%s: rate-limited on account %s (code %s); sleeping %ds "
                "before single retry",
                resource_name,
                account_id,
                facebook_error.api_error_code(),
                wait_seconds,
            )
            time.sleep(wait_seconds)

        try:
            yield from stream(cred, group_name)
        except (ResourceExtractionError, FacebookRequestError) as exc:
            facebook_error = find_rate_limit_cause(exc)
            if facebook_error is None:
                raise
            logging.error(
                "%s: account %s still rate-limited after retry (code %s); "
                "already-yielded items persist via merge but later pages "
                "were not loaded this run.",
                resource_name,
                account_id,
                facebook_error.api_error_code(),
            )
            if on_partial is not None:
                on_partial(account_id)
