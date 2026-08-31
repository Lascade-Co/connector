import dlt

from pipelines.facebook.currency import FX_RATE_COLUMNS
from pipelines.facebook.raw_sources import ads_src, insights_src
from pipelines.facebook.creative_status import mark_partial_creative_account
from pipelines.facebook.rate_limit import stream_with_rate_limit_guard
from pipelines.facebook.structural import load_structural_resource

# ---------------------------------------------------------------------------
# STRUCTURAL OBJECTS
# ---------------------------------------------------------------------------


@dlt.resource(name="ads", primary_key="id", write_disposition="merge")
def ads_all(accounts, group_name: str):
    yield from load_structural_resource(
        accounts,
        group_name,
        source_factory=ads_src,
        source_attribute="ads",
        resource_name="ads",
    )


@dlt.resource(name="campaigns", primary_key="id", write_disposition="merge")
def campaigns_all(accounts, group_name: str, row_transform=None):
    yield from load_structural_resource(
        accounts,
        group_name,
        source_factory=ads_src,
        source_attribute="campaigns",
        resource_name="campaigns",
        row_transform=row_transform,
    )


@dlt.resource(name="ad_sets", primary_key="id", write_disposition="merge")
def adsets_all(accounts, group_name: str, row_transform=None):
    yield from load_structural_resource(
        accounts,
        group_name,
        source_factory=ads_src,
        source_attribute="ad_sets",
        resource_name="ad_sets",
        row_transform=row_transform,
    )


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
    pre_flatten=None,
):
    for cred in accounts:
        # insights_src returns a single DltResource whose name is dynamic
        for r in insights_src(
            cred,
            report_start_date=report_start_date,
            report_end_date=report_end_date,
            pre_flatten=pre_flatten,
        ):
            r["account_id"] = cred["account_id"]
            r["managing_system"] = group_name
            yield r


# ---------------------------------------------------------------------------
# FX AUDIT TRAIL
# ---------------------------------------------------------------------------


@dlt.resource(
    name="fx_daily_rates",
    primary_key=[
        "source_currency",
        "target_currency",
        "requested_date",
        "method_version",
    ],
    write_disposition="merge",
    columns=FX_RATE_COLUMNS,
)
def fx_daily_rates(rows):
    """Record the rates a run actually used. Insights never reads this back."""

    yield from rows


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
