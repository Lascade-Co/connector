import logging
import time

import dlt
from dlt.extract.exceptions import ResourceExtractionError
from facebook_business.exceptions import FacebookRequestError

from pipelines.facebook.raw_sources import ads_src, insights_src
from pipelines.facebook.rate_limit import (
    WAIT_CAP_SECONDS,
    find_rate_limit_cause,
    parse_wait_seconds,
)

# ---------------------------------------------------------------------------
# STRUCTURAL OBJECTS
# ---------------------------------------------------------------------------

@dlt.resource(name="ads", primary_key="id", write_disposition="merge")
def ads_all(accounts, group_name: str):
    for cred in accounts:
        for r in ads_src(cred).ads:            # add fields=... if you like
            r["account_id"] = cred["account_id"]
            r["managing_system"] = group_name
            yield r


@dlt.resource(name="campaigns", primary_key="id", write_disposition="merge")
def campaigns_all(accounts, group_name: str):
    for cred in accounts:
        for r in ads_src(cred).campaigns:
            r["account_id"] = cred["account_id"]
            r["managing_system"] = group_name
            yield r


@dlt.resource(name="ad_sets", primary_key="id", write_disposition="merge")
def adsets_all(accounts, group_name: str):
    for cred in accounts:
        for r in ads_src(cred).ad_sets:
            r["account_id"] = cred["account_id"]
            r["managing_system"] = group_name
            yield r


def _stream_creatives(cred, group_name: str):
    """Iterate ad_creatives for one account, tagging account_id + managing_system."""
    for r in ads_src(cred).ad_creatives:
        r["account_id"] = cred["account_id"]
        r["managing_system"] = group_name
        yield r


@dlt.resource(name="ad_creatives", primary_key="id", write_disposition="merge")
def creatives_all(accounts, group_name: str):
    """Yield creatives across accounts with rate-limit-aware retry.

    Iterating the inner `ads_src(cred).ad_creatives` DltResource routes
    through dlt's pipe iterator, which wraps any non-dlt exception as
    `ResourceExtractionError(...) from ex`. But `ads_src(cred)` itself
    eagerly calls `get_ads_account` for credential lookup, which can raise
    `FacebookRequestError` directly without wrapping. We catch both forms
    and walk `__cause__` to find a rate-limit FB error; everything else
    re-raises so genuine bugs stay loud.

    On a rate-limit hit we read the account's own
    `estimated_time_to_regain_access` header (via `parse_wait_seconds`). If
    the estimate fits inside `WAIT_CAP_SECONDS` we sleep and retry the
    account once; otherwise — or if the retry also rate-limits — we log at
    ERROR and move on. Items already yielded persist via the merge
    disposition, so the next scheduled run resumes by primary key.

    Tradeoff: Meta paginates by an opaque cursor with no server-side
    ordering guarantee, so we can't checkpoint mid-account — a run that
    consistently rate-limits at page N will load pages 1..N and silently
    miss N+1+. Accounts repeatedly hitting this should be triaged (split
    scheduling, lower field set, etc.).
    """
    for cred in accounts:
        account_id = cred["account_id"]
        try:
            yield from _stream_creatives(cred, group_name)
            continue
        except (ResourceExtractionError, FacebookRequestError) as e:
            fb = find_rate_limit_cause(e)
            if fb is None:
                raise
            wait = parse_wait_seconds(fb.http_headers(), default=300)
            if wait > WAIT_CAP_SECONDS:
                logging.error(
                    "ad_creatives: account %s rate-limited (code %s) with "
                    "estimated wait %ds exceeding %ds cap; skipping. "
                    "Already-yielded items persist via merge.",
                    account_id,
                    fb.api_error_code(),
                    wait,
                    WAIT_CAP_SECONDS,
                )
                continue
            logging.warning(
                "ad_creatives: rate-limited on account %s (code %s); "
                "sleeping %ds before single retry",
                account_id,
                fb.api_error_code(),
                wait,
            )
            time.sleep(wait)

        try:
            yield from _stream_creatives(cred, group_name)
        except (ResourceExtractionError, FacebookRequestError) as e:
            fb = find_rate_limit_cause(e)
            if fb is None:
                raise
            logging.error(
                "ad_creatives: account %s still rate-limited after retry "
                "(code %s); already-yielded items persist via merge but "
                "later pages were not loaded this run.",
                account_id,
                fb.api_error_code(),
            )

# ---------------------------------------------------------------------------
# METRIC FACT TABLE
# ---------------------------------------------------------------------------

@dlt.resource(
    name="insights",
    primary_key=["account_id", "date_start", "ad_id"],
    write_disposition="merge",
)
def insights_all(accounts, group_name: str):
    for cred in accounts:
        # insights_src returns a single DltResource whose name is dynamic
        for r in insights_src(cred):
            r["account_id"] = cred["account_id"]
            r["managing_system"] = group_name
            yield r

# ---------------------------------------------------------------------------
# LIST OF RESOURCES
# ---------------------------------------------------------------------------

all_sources = [
    ads_all,
    campaigns_all,
    adsets_all,
    creatives_all,
    insights_all,
]
