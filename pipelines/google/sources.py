import datetime
import logging
import os
from typing import Iterator

import dlt
from dlt.common.typing import TDataItem
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

from pipelines.google.queries import (
    AD_METRICS_QUERY,
    CAMPAIGN_METRICS_QUERY,
    CAMPAIGN_QUERY,
    CONVERSION_ACTION_QUERY,
    CUSTOMER_CLIENT_QUERY,
    run_query,
)
from pipelines.google.date_range import reporting_dates

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
        group_name: str, days_back: int = ROLLING_DAYS,
        *, start_date: str = None, end_date: str = None,
) -> Iterator[TDataItem]:
    start, end = reporting_dates(days_back, start_date=start_date, end_date=end_date)
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
            "ad_id": r.ad_group_ad.ad.id,
            "ad_name": r.ad_group_ad.ad.name,
            "ad_type": r.ad_group_ad.ad.type,
            "ad_status": str(r.ad_group_ad.status.name),
            "ad_primary_status": str(r.ad_group_ad.primary_status.name),
            "ad_primary_status_reason": str(r.ad_group_ad.primary_status_reasons[0].name) if r.ad_group_ad.primary_status_reasons else None,
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
            "video_views": m.video_trueview_views,
        }


@dlt.resource(
    name="google_ads_campaign_metrics",  # final table name
    primary_key=["account_id", "campaign_id", "date"],
    write_disposition="merge",
    columns={
        "cost_micros": {"data_type": "bigint"},
        "impressions": {"data_type": "bigint"},
        "clicks": {"data_type": "bigint"},
        "conversions_by_click_date": {"data_type": "double"},
        "conversions_value_by_click_date": {"data_type": "double"},
        "conversions_by_conversion_date": {"data_type": "double"},
        "conversions_value_by_conversion_date": {"data_type": "double"},
    },
)
def campaign_metrics(
        client: GoogleAdsClient,
        customer_id: str,
        group_name: str, days_back: int = ROLLING_DAYS
) -> Iterator[TDataItem]:
    """
    Fetches campaign-grain spend and performance metrics.

    Exists because ads_metrics reads FROM ad_group_ad, which has no rows for
    Performance Max campaigns, so their spend never loads.
    """
    end = datetime.date.today()
    start = end - datetime.timedelta(days=days_back)
    query = CAMPAIGN_METRICS_QUERY.format(start=start, end=end)

    for r in run_query(client, customer_id, query):
        m = r.metrics

        # Nothing converts Google cost to AED (unlike pipelines/facebook/currency.py),
        # and every downstream consumer sums these as AED.
        if r.customer.currency_code != "AED":
            logging.warning(
                f"Account {r.customer.id} ({r.customer.descriptive_name}) bills in "
                f"{r.customer.currency_code}, not AED - its cost_micros will be summed as AED"
            )

        yield {
            "date": r.segments.date,
            "account_id": r.customer.id,
            "account_name": r.customer.descriptive_name,
            "account_currency_code": r.customer.currency_code,
            "account_time_zone": r.customer.time_zone,
            "managing_system": group_name,
            "campaign_id": r.campaign.id,
            "campaign_name": r.campaign.name,
            "advertising_channel_type": str(r.campaign.advertising_channel_type.name),
            # Core metrics (cost in micros)
            "cost_micros": m.cost_micros,
            "impressions": m.impressions,
            "clicks": m.clicks,
            # Conversion metrics - Click Date Attribution
            "conversions_by_click_date": m.conversions,
            "conversions_value_by_click_date": m.conversions_value,
            # Conversion metrics - Conversion Date Attribution
            "conversions_by_conversion_date": m.conversions_by_conversion_date,
            "conversions_value_by_conversion_date": m.conversions_value_by_conversion_date,
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
        # Target ROAS can be in two different fields depending on bidding strategy
        target_roas_value = r.campaign.target_roas.target_roas or r.campaign.maximize_conversion_value.target_roas or None
        
        yield {
            "account_id": r.customer.id,
            "campaign_id": r.campaign.id,
            "campaign_name": r.campaign.name,
            "bidding_strategy_type": str(r.campaign.bidding_strategy_type.name),
            "target_roas": target_roas_value,
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
        group_name: str, days_back: int = ROLLING_DAYS,
        *, start_date: str = None, end_date: str = None,
) -> Iterator[TDataItem]:
    """
    Fetches conversion metrics broken down by conversion action (e.g., dpl, purchase_web, etc.)
    """
    start, end = reporting_dates(days_back, start_date=start_date, end_date=end_date)
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


@dlt.resource(
    name="google_ads_customer_clients",  # final table name
    primary_key=["account_id"],
    write_disposition="replace",
)
def customer_clients(
        client: GoogleAdsClient,
        mcc_id: str,
        group_name: str
) -> Iterator[TDataItem]:
    """
    Enumerates every account under the MCC, so unsynced accounts and each
    account's currency and time zone are visible in data.

    Never fatal. This is a diagnostic, but it loads in the same pipe.run() as every
    account's spend, and an MCC-scoped query is a path the travel pipeline has never
    exercised - a failure here must not cost us the day's spend.
    """
    try:
        rows = list(run_query(client, mcc_id, CUSTOMER_CLIENT_QUERY))
    except GoogleAdsException as exc:
        logging.warning(f"Could not enumerate accounts under MCC {mcc_id}: {exc}")
        return

    for r in rows:
        cc = r.customer_client

        yield {
            "account_id": cc.id,
            "descriptive_name": cc.descriptive_name,
            "currency_code": cc.currency_code,
            "time_zone": cc.time_zone,
            "is_manager": cc.manager,
            "status": str(cc.status.name),
            "level": cc.level,
            "managing_system": group_name,
        }


all_sources = [
    ads_metrics,
    campaign_metrics,
    campaign_budgets,
    conversion_actions,
]
