import dlt

from pipelines.facebook.raw_sources import ads_src, insights_src
from pipelines.facebook.creative_status import (
    get_partial_creative_accounts,
    mark_partial_creative_account,
    reset_partial_creative_accounts,
)
from pipelines.facebook.rate_limit import stream_with_rate_limit_guard

# ---------------------------------------------------------------------------
# STRUCTURAL OBJECTS
# ---------------------------------------------------------------------------


@dlt.resource(name="ads", primary_key="id", write_disposition="merge")
def ads_all(accounts, group_name: str):
    for cred in accounts:
        for r in ads_src(cred).ads:  # add fields=... if you like
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
    """Park a quota-limited account and let the runner report a partial load."""
    yield from stream_with_rate_limit_guard(
        accounts,
        group_name,
        _stream_creatives,
        resource_name="ad_creatives",
        on_partial=mark_partial_creative_account,
    )


# ---------------------------------------------------------------------------
# METRIC FACT TABLE
# ---------------------------------------------------------------------------


@dlt.resource(
    name="insights",
    primary_key=["account_id", "date_start", "ad_id"],
    write_disposition="merge",
)
def insights_all(
    accounts,
    group_name: str,
    *,
    report_start_date=None,
    report_end_date=None,
):
    for cred in accounts:
        # insights_src returns a single DltResource whose name is dynamic
        for r in insights_src(
            cred,
            report_start_date=report_start_date,
            report_end_date=report_end_date,
        ):
            r["account_id"] = cred["account_id"]
            r["managing_system"] = group_name
            yield r


# ---------------------------------------------------------------------------
# LIST OF RESOURCES
# ---------------------------------------------------------------------------

all_sources = [
    insights_all,
    ads_all,
    campaigns_all,
    adsets_all,
    creatives_all,
]
