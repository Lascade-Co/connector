import dlt
from pipelines.subscription_facebook.raw_sources import ads_src, insights_src

# ---------------------------------------------------------------------------
# STRUCTURAL OBJECTS
# ---------------------------------------------------------------------------

@dlt.resource(name="ads", primary_key="id", write_disposition="merge")
def ads_all(accounts, group_name: str):
    for cred in accounts:
        for r in ads_src(cred).ads:
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


@dlt.resource(name="ad_creatives", primary_key="id", write_disposition="merge")
def creatives_all(accounts, group_name: str):
    for cred in accounts:
        for r in ads_src(cred).ad_creatives:
            r["account_id"] = cred["account_id"]
            r["managing_system"] = group_name
            yield r

# ---------------------------------------------------------------------------
# METRIC FACT TABLE
# ---------------------------------------------------------------------------

def _subscribe_revenue(conversion_values):
    # Mirrors the reference Django command: sum conversion_values where
    # action_type == "subscribe_mobile_app" or label contains "subscribe".
    if not isinstance(conversion_values, list):
        return 0.0
    total = 0.0
    for item in conversion_values:
        if not isinstance(item, dict):
            continue
        at = (item.get("action_type") or "").lower()
        label = (item.get("label") or "").lower()
        if at == "subscribe_mobile_app" or "subscribe" in label:
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
def insights_all(accounts, group_name: str):
    for cred in accounts:
        for r in insights_src(cred):
            r["account_id"] = cred["account_id"]
            r["managing_system"] = group_name
            r["subscription_revenue"] = _subscribe_revenue(r.get("conversion_values"))
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
