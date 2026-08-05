"""Pipeline-level handling for Meta Graph API quota failures."""

import logging
from typing import Callable, Iterable, Mapping, Optional

from dlt.extract.exceptions import ResourceExtractionError
from facebook_business.exceptions import FacebookRequestError

from facebook_ads.rate_limit import RATE_LIMIT_CODES, parse_usage_headers


def _meta_reset_seconds(headers: Mapping[str, str]) -> Optional[int]:
    """Return Meta's longest parsed reset duration in seconds, if provided."""

    snapshot = parse_usage_headers(headers)
    return None if snapshot is None else snapshot.reset_seconds


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
    meta_seconds = _meta_reset_seconds(headers or {})
    seconds = meta_seconds if meta_seconds is not None else default
    return max(1, seconds)


def stream_with_rate_limit_guard(
    accounts: Iterable[dict],
    group_name: str,
    stream: Callable,
    *,
    resource_name: str,
    on_partial: Optional[Callable[[str], None]] = None,
    default_wait_seconds: int = 300,
):
    """Park a throttled account without retrying or blocking other work.

    Meta advises clients to stop calling after a quota failure. Already-yielded
    items remain eligible for the current dlt load, while ``on_partial`` lets
    the runner fail visibly after committing successful work.
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
            meta_reset_seconds = _meta_reset_seconds(
                facebook_error.http_headers() or {}
            )
            error_subcode = facebook_error.api_error_subcode()
            if meta_reset_seconds is None:
                wait_detail = f"policy fallback cooldown {wait_seconds}s"
            else:
                wait_detail = f"Meta reset estimate {wait_seconds}s"
            logging.error(
                "%s: account %s rate-limited (code %s, subcode %s); parking "
                "it for this run (%s). Already-yielded items remain eligible "
                "for merge.",
                resource_name,
                account_id,
                facebook_error.api_error_code(),
                error_subcode if error_subcode is not None else "unknown",
                wait_detail,
            )
            if on_partial is not None:
                on_partial(account_id)


# Temporary compatibility alias for integrations importing the old name.
stream_with_rate_limit_retry = stream_with_rate_limit_guard
