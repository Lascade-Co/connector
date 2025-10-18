"""
GA4 data sources and resources for the pipeline.
Following the same pattern as Google Ads pipeline.
"""
import datetime
import os
from typing import Callable, Iterator, List

import dlt
from dlt.common.typing import TDataItem
from dlt.sources.credentials import GcpOAuthCredentials, GcpServiceAccountCredentials
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import Dimension, Metric

from google_analytics.helpers.data_processing import get_report
from utils import date_key_from_ga4

ROLLING_DAYS = 120


def get_days_back() -> int:
    """Get days_back from environment variable for backfill, or use default."""
    backfill_days = os.getenv("GA4_BACKFILL_DAYS")
    if backfill_days:
        try:
            days_int = int(backfill_days)
            if days_int > 0:
                return days_int
        except ValueError:
            pass
    return ROLLING_DAYS


def get_client(
        credentials: GcpOAuthCredentials | GcpServiceAccountCredentials,
) -> BetaAnalyticsDataClient:
    """
    Create and return a GA4 BetaAnalyticsDataClient.
    
    Args:
        credentials: GCP OAuth or Service Account credentials
        
    Returns:
        BetaAnalyticsDataClient instance
    """
    # Generate access token for credentials if we are using OAuth2.0
    if isinstance(credentials, GcpOAuthCredentials):
        credentials.auth("https://www.googleapis.com/auth/analytics.readonly")

    # Build the service object for Google Analytics API
    return BetaAnalyticsDataClient(credentials=credentials.to_native_credentials())


def _create_report_resource(
        name: str,
        primary_key: List[str],
        dimensions: List[Dimension],
        metrics: List[Metric],
        row_mapper: Callable[[dict, int, str], TDataItem],
        doc: str,
) -> Callable[[BetaAnalyticsDataClient, int, str, int], Iterator[TDataItem]]:
    @dlt.resource(name=name, primary_key=primary_key, write_disposition="merge")
    def resource(
            client: BetaAnalyticsDataClient,
            property_id: int,
            group_name: str,
            days_back: int = ROLLING_DAYS,
    ) -> Iterator[TDataItem]:
        end_date = datetime.date.today() - datetime.timedelta(days=1)
        start_date = end_date - datetime.timedelta(days=days_back)

        for row in get_report(
                client=client,
                property_id=property_id,
                dimension_list=dimensions,
                metric_list=metrics,
                limit=10000,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
        ):
            yield row_mapper(row, property_id, group_name)

    resource.__doc__ = doc
    return resource


traffic_sources = _create_report_resource(
    name="ga4_traffic_sources",
    primary_key=["property_id", "date", "platform", "source", "medium"],
    dimensions=[
        Dimension(name="date"),
        Dimension(name="platform"),
        Dimension(name="source"),
        Dimension(name="medium"),
        Dimension(name="campaignName"),
    ],
    metrics=[
        Metric(name="conversions"),
        Metric(name="totalRevenue"),
    ],
    row_mapper=lambda row, property_id, group_name: {
        "property_id": str(property_id),
        "managing_system": group_name,
        "date": row.get("date"),
        "date_key_pt": date_key_from_ga4(row.get("date")),
        "platform": row.get("platform"),
        "source": row.get("source"),
        "medium": row.get("medium"),
        "campaign": row.get("campaignName"),
        "conversions": float(row.get("conversions_INTEGER", 0)),
        "total_revenue": float(row.get("totalRevenue_CURRENCY", 0)),
    },
    doc="""Fetches traffic source data from GA4. Includes sessions, users, and conversions by source/medium.""",
)

session_traffic_sources = _create_report_resource(
    name="ga4_session_traffic_sources",
    primary_key=["property_id", "date", "platform", "session_source", "session_medium"],
    dimensions=[
        Dimension(name="date"),
        Dimension(name="platform"),
        Dimension(name="sessionSource"),
        Dimension(name="sessionMedium"),
        Dimension(name="sessionCampaignName"),
    ],
    metrics=[
        Metric(name="sessions"),
        Metric(name="totalUsers"),
        Metric(name="newUsers"),
        Metric(name="screenPageViews"),
    ],
    row_mapper=lambda row, property_id, group_name: {
        "property_id": str(property_id),
        "managing_system": group_name,
        "date": row.get("date"),
        "date_key_pt": date_key_from_ga4(row.get("date")),
        "platform": row.get("platform"),
        "session_source": row.get("sessionSource"),
        "session_medium": row.get("sessionMedium"),
        "session_campaign": row.get("sessionCampaignName"),
        "sessions": int(row.get("sessions_INTEGER", 0)),
        "total_users": int(row.get("totalUsers_INTEGER", 0)),
        "new_users": int(row.get("newUsers_INTEGER", 0)),
        "screen_page_views": int(row.get("screenPageViews_INTEGER", 0)),
    },
    doc="""Fetches session-level traffic source data from GA4. Includes sessions, users, and pageviews by source/medium (session-scoped).""",
)

user_engagement = _create_report_resource(
    name="ga4_user_engagement",
    primary_key=["property_id", "date", "platform"],
    dimensions=[
        Dimension(name="date"),
        Dimension(name="platform"),
    ],
    metrics=[
        Metric(name="sessions"),
        Metric(name="engagementRate"),
        Metric(name="averageSessionDuration"),
        Metric(name="bounceRate"),
        Metric(name="screenPageViewsPerSession"),
        Metric(name="eventCount"),
    ],
    row_mapper=lambda row, property_id, group_name: {
        "property_id": str(property_id),
        "managing_system": group_name,
        "date": row.get("date"),
        "date_key_pt": date_key_from_ga4(row.get("date")),
        "platform": row.get("platform"),
        "sessions": int(row.get("sessions_INTEGER", 0)),
        "engagement_rate": float(row.get("engagementRate_FLOAT", 0)),
        "average_session_duration": float(row.get("averageSessionDuration_SECONDS", 0)),
        "bounce_rate": float(row.get("bounceRate_FLOAT", 0)),
        "screen_page_views_per_session": float(row.get("screenPageViewsPerSession_FLOAT", 0)),
        "event_count": int(row.get("eventCount_INTEGER", 0)),
    },
    doc="""Fetches user engagement metrics from GA4. Includes engagement rate, session duration, etc.""",
)

device_category = _create_report_resource(
    name="ga4_device_category",
    primary_key=["property_id", "date", "platform", "device_category"],
    dimensions=[
        Dimension(name="date"),
        Dimension(name="platform"),
        Dimension(name="deviceCategory"),
    ],
    metrics=[
        Metric(name="sessions"),
        Metric(name="totalUsers"),
        Metric(name="conversions"),
        Metric(name="totalRevenue"),
    ],
    row_mapper=lambda row, property_id, group_name: {
        "property_id": str(property_id),
        "managing_system": group_name,
        "date": row.get("date"),
        "date_key_pt": date_key_from_ga4(row.get("date")),
        "platform": row.get("platform"),
        "device_category": row.get("deviceCategory"),
        "sessions": int(row.get("sessions_INTEGER", 0)),
        "total_users": int(row.get("totalUsers_INTEGER", 0)),
        "conversions": float(row.get("conversions_INTEGER", 0)),
        "total_revenue": float(row.get("totalRevenue_CURRENCY", 0)),
    },
    doc="""Fetches device category breakdown from GA4.""",
)

users = _create_report_resource(
    name="ga4_users",
    primary_key=["property_id", "date", "platform", "new_vs_returning"],
    dimensions=[
        Dimension(name="date"),
        Dimension(name="platform"),
        Dimension(name="newVsReturning"),
    ],
    metrics=[
        Metric(name="activeUsers"),
    ],
    row_mapper=lambda row, property_id, group_name: {
        "property_id": str(property_id),
        "managing_system": group_name,
        "date": row.get("date"),
        "date_key_pt": date_key_from_ga4(row.get("date")),
        "platform": row.get("platform"),
        "new_vs_returning": row.get("newVsReturning"),
        "active_users": int(row.get("activeUsers_INTEGER", 0)),
    },
    doc="""Fetches active users split by new vs returning from GA4.""",
)

events = _create_report_resource(
    name="ga4_events",
    primary_key=["property_id", "date", "platform", "event_name"],
    dimensions=[
        Dimension(name="date"),
        Dimension(name="platform"),
        Dimension(name="eventName"),
    ],
    metrics=[
        Metric(name="eventCount"),
        Metric(name="eventCountPerUser"),
        Metric(name="eventValue"),
        Metric(name="totalUsers"),
    ],
    row_mapper=lambda row, property_id, group_name: {
        "property_id": str(property_id),
        "managing_system": group_name,
        "date": row.get("date"),
        "date_key_pt": date_key_from_ga4(row.get("date")),
        "platform": row.get("platform"),
        "event_name": row.get("eventName"),
        "event_count": int(row.get("eventCount_INTEGER", 0)),
        "event_count_per_user": float(row.get("eventCountPerUser_FLOAT", 0)),
        "event_value": float(row.get("eventValue_CURRENCY", 0)),
        "total_users": int(row.get("totalUsers_INTEGER", 0)),
    },
    doc="""Fetches event data from GA4.""",
)

all_sources = [
    traffic_sources,
    session_traffic_sources,
    user_engagement,
    device_category,
    users,
    events,
]
