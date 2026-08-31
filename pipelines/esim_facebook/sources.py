import dlt

from pipelines.facebook.creative_status import mark_partial_creative_account
from pipelines.facebook.rate_limit import stream_with_rate_limit_guard
from pipelines.facebook.structural import load_structural_resource
from pipelines.esim_facebook.raw_sources import ads_src, insights_src

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


def _stream_creatives(cred, group_name: str):
    """Iterate creatives for one account and attach pipeline dimensions."""
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
    columns={
        "date_start": {"data_type": "date"},
        "date_stop": {"data_type": "date"},
        "reach": {"data_type": "bigint"},
        "clicks": {"data_type": "bigint"},
        "unique_clicks": {"data_type": "bigint"},
        "impressions": {"data_type": "bigint"},
        "cpc": {"data_type": "double"},
        "cpm": {"data_type": "double"},
        "cpp": {"data_type": "double"},
        "ctr": {"data_type": "double"},
        "unique_ctr": {"data_type": "double"},
        "frequency": {"data_type": "double"},
    },
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
