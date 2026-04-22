import datetime
import os
from typing import Iterator

import dlt
from dlt.common.typing import TDataItem
from google.ads.googleads.client import GoogleAdsClient

from pipelines.google.queries import run_query
from pipelines.google.sources import (
    campaign_budgets,
    conversion_actions,
    ROLLING_DAYS,
)


def get_days_back() -> int:
    backfill_days = os.getenv("SUB_GOOGLE_BACKFILL_DAYS")
    if backfill_days:
        try:
            days_int = int(backfill_days)
            if days_int > 0:
                return days_int
        except ValueError:
            pass
    return ROLLING_DAYS


# Same as pipelines/google/queries.AD_METRICS_QUERY plus campaign.bidding_strategy_type
# so we can derive ROAS-only revenue per row.
AD_METRICS_QUERY = """
SELECT
    segments.date,
    customer.id,
    customer.descriptive_name,
    campaign.id,
    campaign.name,
    campaign.bidding_strategy_type,
    ad_group.id,
    ad_group.name,
    ad_group_ad.ad.id,
    ad_group_ad.ad.name,
    ad_group_ad.ad.type,
    ad_group_ad.status,
    ad_group_ad.primary_status,
    ad_group_ad.primary_status_reasons,
    metrics.cost_micros,
    metrics.impressions,
    metrics.clicks,
    metrics.ctr,
    metrics.average_cpc,
    metrics.average_cpm,
    metrics.conversions_by_conversion_date,
    metrics.conversions_value_by_conversion_date,
    metrics.conversions,
    metrics.conversions_value,
    metrics.cost_per_conversion,
    metrics.all_conversions,
    metrics.all_conversions_value,
    metrics.video_views
FROM ad_group_ad
WHERE segments.date BETWEEN '{start}' AND '{end}'
"""


@dlt.resource(
    name="google_ads",
    primary_key=["account_id", "ad_id", "date"],
    write_disposition="merge",
    columns={
        "installs": {"data_type": "double"},
        "revenue": {"data_type": "double"},
    },
)
def ads_metrics(
    client: GoogleAdsClient,
    customer_id: str,
    group_name: str,
    days_back: int = ROLLING_DAYS,
) -> Iterator[TDataItem]:
    end = datetime.date.today()
    start = end - datetime.timedelta(days=days_back)
    query = AD_METRICS_QUERY.format(start=start, end=end)

    for r in run_query(client, customer_id, query):
        m = r.metrics
        bidding = str(r.campaign.bidding_strategy_type.name) if r.campaign.bidding_strategy_type else ""
        installs = m.conversions or 0
        revenue = (m.conversions_value or 0) if "ROAS" in bidding else 0

        yield {
            "date": r.segments.date,
            "account_id": r.customer.id,
            "account_name": r.customer.descriptive_name,
            "managing_system": group_name,
            "campaign_id": r.campaign.id,
            "campaign_name": r.campaign.name,
            "bidding_strategy_type": bidding,
            "ad_group_id": r.ad_group.id,
            "ad_group_name": r.ad_group.name,
            "ad_id": r.ad_group_ad.ad.id,
            "ad_name": r.ad_group_ad.ad.name,
            "ad_type": r.ad_group_ad.ad.type,
            "ad_status": str(r.ad_group_ad.status.name),
            "ad_primary_status": str(r.ad_group_ad.primary_status.name),
            "ad_primary_status_reason": str(r.ad_group_ad.primary_status_reasons[0].name) if r.ad_group_ad.primary_status_reasons else None,
            "cost_micros": m.cost_micros,
            "impressions": m.impressions,
            "clicks": m.clicks,
            "ctr": m.ctr,
            "average_cpc": m.average_cpc,
            "average_cpm": m.average_cpm,
            "conversions_by_conversion_date": m.conversions_by_conversion_date,
            "conversions_value_by_conversion_date": m.conversions_value_by_conversion_date,
            "conversions_by_click_date": m.conversions,
            "conversions_value_by_click_date": m.conversions_value,
            "cost_per_conversion": m.cost_per_conversion,
            "all_conversions": m.all_conversions,
            "all_conversions_value": m.all_conversions_value,
            "video_views": m.video_views,
            "installs": float(installs),
            "revenue": float(revenue),
        }


all_sources = [ads_metrics, campaign_budgets, conversion_actions]
