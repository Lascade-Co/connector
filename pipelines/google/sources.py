import datetime
from typing import Iterator

import dlt
from dlt.common.typing import TDataItem
from google.ads.googleads.client import GoogleAdsClient

from pipelines.google.queries import AD_METRICS_QUERY, run_query

ROLLING_DAYS = 30


@dlt.resource(
    name="google_ads",  # final table name
    primary_key=["account_id", "ad_id", "date"],
    write_disposition="merge",
)
def ads_metrics(
        client: GoogleAdsClient,
        customer_id: str,
        group_name: str, days_back: int = ROLLING_DAYS
) -> Iterator[TDataItem]:
    end = datetime.date.today()
    start = end - datetime.timedelta(days=days_back)
    query = AD_METRICS_QUERY.format(start=start, end=end)

    for r in run_query(client, customer_id, query):
        m = r.metrics
        yield {
            "date": r.segments.date,
            "account_id": r.customer.id,
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
            "average_cpm": m.average_cpm,
            "spend_micros": m.cost_micros,
            "conversion_value": m.conversions_value_by_conversion_date,
            "conversions": m.conversions_by_conversion_date,
            "video_views": m.video_views,
        }


all_sources = [
    ads_metrics,
]
