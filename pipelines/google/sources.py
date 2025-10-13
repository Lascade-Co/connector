import datetime
import os
from typing import Iterator

import dlt
from dlt.common.typing import TDataItem
from google.ads.googleads.client import GoogleAdsClient

from pipelines.google.queries import AD_METRICS_QUERY, CAMPAIGN_QUERY, CONVERSION_ACTION_QUERY, run_query

ROLLING_DAYS = 30


def get_days_back() -> int:
    """Get days_back from environment variable for backfill, or use default."""
    backfill_days = os.getenv("GOOGLE_BACKFILL_DAYS")
    if backfill_days:
        try:
            days_int = int(backfill_days)
            if days_int > 0:
                return days_int
        except ValueError:
            pass
    return ROLLING_DAYS


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
            "account_name": r.customer.descriptive_name,
            "managing_system": group_name,
            "campaign_id": r.campaign.id,
            "campaign_name": r.campaign.name,
            "ad_group_id": r.ad_group.id,
            "ad_group_name": r.ad_group.name,
            "target_roas": r.campaign.maximize_conversion_value.target_roas if r.campaign.maximize_conversion_value.target_roas else 0,
            "ad_id": r.ad_group_ad.ad.id,
            "ad_name": r.ad_group_ad.ad.name,
            "ad_type": r.ad_group_ad.ad.type,
            # Core metrics (converted from micros)
            "cost_micros": m.cost_micros,
            "impressions": m.impressions,
            "clicks": m.clicks,
            "ctr": m.ctr,
            "average_cpc": m.average_cpc,
            "average_cpm": m.average_cpm,
            # Conversion metrics - Conversion Date Attribution
            "conversions_by_conversion_date": m.conversions_by_conversion_date,
            "conversions_value_by_conversion_date": m.conversions_value_by_conversion_date,
            # Conversion metrics - Click Date Attribution
            "conversions_by_click_date": m.conversions,
            "conversions_value_by_click_date": m.conversions_value,
            # Other conversion metrics
            "cost_per_conversion": m.cost_per_conversion,
            "all_conversions": m.all_conversions,
            "all_conversions_value": m.all_conversions_value,
            # Additional metrics
            "video_views": m.video_views,
        }


@dlt.resource(
    name="google_ads_campaigns",  # final table name
    primary_key=["account_id", "campaign_id"],
    write_disposition="replace",
)
def campaign_budgets(
        client: GoogleAdsClient,
        customer_id: str,
        group_name: str
) -> Iterator[TDataItem]:
    """
    Fetches campaign budget information.
    """
    for r in run_query(client, customer_id, CAMPAIGN_QUERY):
        yield {
            "account_id": r.customer.id,
            "campaign_id": r.campaign.id,
            "campaign_name": r.campaign.name,
            "daily_budget": r.campaign_budget.amount_micros / 1000000,
            "managing_system": group_name,
        }


@dlt.resource(
    name="google_ads_conversion_actions",  # final table name
    primary_key=["account_id", "ad_id", "date", "conversion_action"],
    write_disposition="merge",
)
def conversion_actions(
        client: GoogleAdsClient,
        customer_id: str,
        group_name: str, days_back: int = ROLLING_DAYS
) -> Iterator[TDataItem]:
    """
    Fetches conversion metrics broken down by conversion action (e.g., dpl, purchase_web, etc.)
    """
    end = datetime.date.today()
    start = end - datetime.timedelta(days=days_back)
    query = CONVERSION_ACTION_QUERY.format(start=start, end=end)

    for r in run_query(client, customer_id, query):
        m = r.metrics
        
        yield {
            "date": r.segments.date,
            "conversion_action": r.segments.conversion_action,
            "conversion_action_name": r.segments.conversion_action_name,
            "account_id": r.customer.id,
            "account_name": r.customer.descriptive_name,
            "managing_system": group_name,
            "campaign_id": r.campaign.id,
            "campaign_name": r.campaign.name,
            "ad_group_id": r.ad_group.id,
            "ad_group_name": r.ad_group.name,
            "ad_id": r.ad_group_ad.ad.id,
            "ad_name": r.ad_group_ad.ad.name,
            # Conversion metrics by action
            "conversions": m.conversions,
            "conversions_value": m.conversions_value,
            "all_conversions": m.all_conversions,
            "all_conversions_value": m.all_conversions_value,
        }


all_sources = [
    ads_metrics,
    campaign_budgets,
    conversion_actions,
]
