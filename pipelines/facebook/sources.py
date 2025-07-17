import dlt
from pipelines.facebook.raw_sources import ads_src, insights_src

# ---------------------------------------------------------------------------
# STRUCTURAL OBJECTS
# ---------------------------------------------------------------------------

@dlt.resource(name="ads", primary_key="id", write_disposition="merge")
def ads_all(accounts):
    for cred in accounts:
        for r in ads_src(cred).ads:            # add fields=... if you like
            r["account_id"] = cred["account_id"]
            yield r


@dlt.resource(name="campaigns", primary_key="id", write_disposition="merge")
def campaigns_all(accounts):
    for cred in accounts:
        for r in ads_src(cred).campaigns:
            r["account_id"] = cred["account_id"]
            yield r


@dlt.resource(name="ad_sets", primary_key="id", write_disposition="merge")
def adsets_all(accounts):
    for cred in accounts:
        for r in ads_src(cred).ad_sets:
            r["account_id"] = cred["account_id"]
            yield r


@dlt.resource(name="ad_creatives", primary_key="id", write_disposition="merge")
def creatives_all(accounts):
    for cred in accounts:
        for r in ads_src(cred).ad_creatives:
            r["account_id"] = cred["account_id"]
            yield r

# ---------------------------------------------------------------------------
# METRIC FACT TABLE
# ---------------------------------------------------------------------------

@dlt.resource(
    name="insights",
    primary_key=["account_id", "date_start", "ad_id"],
    write_disposition="merge",
)
def insights_all(accounts):
    for cred in accounts:
        for r in insights_src(cred).facebook_insights:
            # r already carries ad_id/adset_id/campaign_id/date_start/…
            r["account_id"] = cred["account_id"]
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
