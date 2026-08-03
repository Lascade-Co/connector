import dlt

from pipelines.facebook.rate_limit import stream_with_rate_limit_retry
from pipelines.esim_facebook.raw_sources import ads_src, insights_src


_partial_creative_accounts: set[str] = set()


def reset_partial_creative_accounts() -> None:
    _partial_creative_accounts.clear()


def get_partial_creative_accounts() -> tuple[str, ...]:
    return tuple(sorted(_partial_creative_accounts))

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
    """Iterate creatives for one account and attach pipeline dimensions."""
    for r in ads_src(cred).ad_creatives:
        r["account_id"] = cred["account_id"]
        r["managing_system"] = group_name
        yield r


@dlt.resource(name="ad_creatives", primary_key="id", write_disposition="merge")
def creatives_all(accounts, group_name: str):
    """Yield creatives with one Meta rate-limit-aware retry per account.

    A wait reported by Meta is honored up to the shared 30-minute cap. Longer
    waits and a second throttle mark only the affected account incomplete;
    non-rate-limit failures still abort immediately.

    Retrying restarts the account from its first page. Duplicate creatives are
    deduplicated at the destination because this resource merges on creative
    ID. The pipeline raises after all account loads if any account remains
    incomplete, making the partial result visible to automation.
    """
    yield from stream_with_rate_limit_retry(
        accounts,
        group_name,
        _stream_creatives,
        resource_name="ad_creatives",
        on_partial=_partial_creative_accounts.add,
    )

# ---------------------------------------------------------------------------
# METRIC FACT TABLE
# ---------------------------------------------------------------------------

@dlt.resource(
    name="insights",
    primary_key=["account_id", "date_start", "ad_id"],
    write_disposition="merge",
    columns={
        "date_start":    {"data_type": "date"},
        "date_stop":     {"data_type": "date"},
        "reach":         {"data_type": "bigint"},
        "clicks":        {"data_type": "bigint"},
        "unique_clicks": {"data_type": "bigint"},
        "impressions":   {"data_type": "bigint"},
        "cpc":           {"data_type": "double"},
        "cpm":           {"data_type": "double"},
        "cpp":           {"data_type": "double"},
        "ctr":           {"data_type": "double"},
        "unique_ctr":    {"data_type": "double"},
        "frequency":     {"data_type": "double"},
    },
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
