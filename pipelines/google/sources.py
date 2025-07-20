import datetime
from typing import List, Iterator

import dlt
from dlt.common.typing import TDataItem
from google.ads.googleads.client import GoogleAdsClient

from pipelines.google.queries import AD_METRICS_QUERY, run_query, CREATIVES_QUERY

ROLLING_DAYS = 30


@dlt.resource(
    name="google_ads",  # final table name
    primary_key=["account_id", "ad_id", "date"],
    write_disposition="merge",
)
def ads_metrics(
        client: GoogleAdsClient,
        customer_ids: List[str],
        group_name: str, days_back: int = ROLLING_DAYS
) -> Iterator[TDataItem]:
    end = datetime.date.today()
    start = end - datetime.timedelta(days=days_back)
    query = AD_METRICS_QUERY.format(start=start, end=end)

    for customer_id in customer_ids:
        for r in run_query(client, customer_id, query):
            m = r.metrics
            yield {
                "date": r.segments.date.value,
                "account_id": customer_id,
                "managing_system": group_name,
                "campaign_id": r.campaign.id,
                "campaign_name": r.campaign.name,
                "ad_group_id": r.ad_group.id,
                "ad_group_name": r.ad_group.name,
                "ad_id": r.ad_group_ad.ad.id,
                "ad_name": r.ad_group_ad.ad.name,
                "impressions": m.impressions,
                "clicks": m.clicks,
                "ctr": m.ctr,
                "average_cpc": m.average_cpc,
                "cpm": m.cost_per_thousand_impressions,
                "spend_micros": m.cost_micros,
                "conversion_value": m.conversions_value,
                "roas": m.conversions_value_per_cost,
                "reach": m.reach,
            }


@dlt.resource(
    name="google_creatives",
    primary_key="ad_id",
    write_disposition="merge",
)
def creatives(
        client: GoogleAdsClient,
        accounts: List[str],
        group_name: str
) -> Iterator[TDataItem]:
    for acc in accounts:
        for r in run_query(client, acc, CREATIVES_QUERY):
            ad = r.ad_group_ad.ad
            yield {
                "account_id": acc,
                "managing_system": group_name,
                "ad_id": ad.id,
                "ad_name": ad.name,
                "ad_type": ad.type.name,
                "final_urls": list(ad.final_urls),
            }


all_sources = [
    ads_metrics,
    creatives,
]
