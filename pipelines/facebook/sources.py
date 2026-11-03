import dlt

from pipelines.facebook.currency import FX_RATE_COLUMNS
from pipelines.facebook.raw_sources import (
    ads_src,
    country_insights_src,
    insights_src,
)
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


# A new table, so these hints take effect (unlike fb___insights, whose existing
# columns dlt never retypes). account_id/campaign_id stay text to join with it.
COUNTRY_INSIGHT_COLUMNS = {
    "spend": {"data_type": "double"},
    "spend_source": {"data_type": "double"},
    "ctr": {"data_type": "double"},
    "impressions": {"data_type": "bigint"},
    "clicks": {"data_type": "bigint"},
    "date_start": {"data_type": "date"},
    "date_stop": {"data_type": "date"},
}

# The flattened actions_*/action_values_* scalars (already AED) carry what the
# report needs, so the arrays would only add child tables. At campaign level the
# ad set/ad ids are placeholders, and the trial/subscription scalars are always
# 0 because conversions are not requested.
_COUNTRY_DROPPED_FIELDS = (
    "actions",
    "action_values",
    "adset_id",
    "ad_id",
    "trial_starts",
    "subscriptions",
    "trial_start_value",
    "subscription_value",
)


@dlt.resource(
    name="insights_country",
    primary_key=["account_id", "campaign_id", "date_start", "country"],
    write_disposition="merge",
    columns=COUNTRY_INSIGHT_COLUMNS,
)
def insights_country(
    accounts,
    group_name: str,
    *,
    report_start_date=None,
    report_end_date=None,
    pre_flatten=None,
):
    for cred in accounts:
        for r in country_insights_src(
            cred,
            report_start_date=report_start_date,
            report_end_date=report_end_date,
            pre_flatten=pre_flatten,
        ):
            for field in _COUNTRY_DROPPED_FIELDS:
                r.pop(field, None)
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
]
