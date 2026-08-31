import dlt
from pipelines.facebook.creative_status import mark_partial_creative_account
from pipelines.facebook.rate_limit import stream_with_rate_limit_guard
from pipelines.facebook.structural import load_structural_resource
from pipelines.subscription_facebook.raw_sources import ads_src, insights_src

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
def campaigns_all(accounts, group_name: str):
    yield from load_structural_resource(
        accounts,
        group_name,
        source_factory=ads_src,
        source_attribute="campaigns",
        resource_name="campaigns",
    )


@dlt.resource(name="ad_sets", primary_key="id", write_disposition="merge")
def adsets_all(accounts, group_name: str):
    yield from load_structural_resource(
        accounts,
        group_name,
        source_factory=ads_src,
        source_attribute="ad_sets",
        resource_name="ad_sets",
    )


@dlt.resource(name="ad_creatives", primary_key="id", write_disposition="merge")
def creatives_all(accounts, group_name: str):
    yield from stream_with_rate_limit_guard(
        accounts,
        group_name,
        _stream_creatives,
        resource_name="ad_creatives",
        on_partial=mark_partial_creative_account,
    )


def _stream_creatives(cred, group_name: str):
    for row in ads_src(cred).ad_creatives:
        row["account_id"] = cred["account_id"]
        row["managing_system"] = group_name
        yield row


# ---------------------------------------------------------------------------
# METRIC FACT TABLE
# ---------------------------------------------------------------------------


def _subscribe_revenue(conversion_values):
    """Preserve the legacy custom-conversion revenue contract."""

    if not isinstance(conversion_values, list):
        return 0.0
    total = 0.0
    for item in conversion_values:
        if not isinstance(item, dict):
            continue
        action_type = (item.get("action_type") or "").lower()
        label = (item.get("label") or "").lower()
        if action_type == "subscribe_mobile_app" or "subscribe" in label:
            try:
                total += float(item.get("value") or 0.0)
            except (TypeError, ValueError):
                continue
    return round(total, 4)


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
        for r in insights_src(
            cred,
            report_start_date=report_start_date,
            report_end_date=report_end_date,
        ):
            r["account_id"] = cred["account_id"]
            r["managing_system"] = group_name
            r["subscription_revenue"] = _subscribe_revenue(
                r.get("conversion_values")
            )
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
