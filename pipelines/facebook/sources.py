import dlt

from pipelines.facebook.utils import ads_src, insights_src


@dlt.resource(name="ads", primary_key="id", write_disposition="merge")
def ads_all(accounts):
    for cred in accounts:
        for r in ads_src(cred).ads:
            r["account_id"] = cred["account_id"]
            yield r


@dlt.resource(
    name="insights",
    primary_key=["account_id", "date_start", "ad_id"],
    write_disposition="merge")
def insights_all(accounts):
    for cred in accounts:
        for r in insights_src(cred).facebook_insights:
            r["account_id"] = cred["account_id"]
            yield r


all_sources = [ads_all, insights_all]
