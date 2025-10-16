"""
GA4 data sources and resources for the pipeline.
Following the same pattern as Google Ads pipeline.
"""
import datetime
import os
from typing import Iterator

import dlt
from dlt.common.typing import TDataItem
from dlt.sources.credentials import GcpOAuthCredentials, GcpServiceAccountCredentials
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import Dimension, Metric

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


@dlt.resource(
    name="ga4_traffic_sources",
    primary_key=["property_id", "date", "source", "medium"],
    write_disposition="merge",
)
def traffic_sources(
    client: BetaAnalyticsDataClient,
    property_id: int,
    group_name: str,
    days_back: int = ROLLING_DAYS,
) -> Iterator[TDataItem]:
    """
    Fetches traffic source data from GA4.
    Includes sessions, users, and conversions by source/medium.
    """
    from google_analytics.helpers.data_processing import get_report
    
    end_date = datetime.date.today() - datetime.timedelta(days=1)  # Yesterday
    start_date = end_date - datetime.timedelta(days=days_back)
    
    dimensions = [
        Dimension(name="date"),
        Dimension(name="source"),
        Dimension(name="medium"),
        Dimension(name="campaignName"),
    ]
    
    metrics = [
        Metric(name="conversions"),
        Metric(name="totalRevenue"),
    ]
    
    for row in get_report(
        client=client,
        property_id=property_id,
        dimension_list=dimensions,
        metric_list=metrics,
        limit=10000,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
    ):
        yield {
            "property_id": str(property_id),
            "managing_system": group_name,
            "date": row.get("date"),
            "source": row.get("source"),
            "medium": row.get("medium"),
            "campaign": row.get("campaignName"),
            "conversions": float(row.get("conversions_INTEGER", 0)),
            "total_revenue": float(row.get("totalRevenue_CURRENCY", 0)),
        }


@dlt.resource(
    name="ga4_session_traffic_sources",
    primary_key=["property_id", "date", "session_source", "session_medium"],
    write_disposition="merge",
)
def session_traffic_sources(
    client: BetaAnalyticsDataClient,
    property_id: int,
    group_name: str,
    days_back: int = ROLLING_DAYS,
) -> Iterator[TDataItem]:
    """
    Fetches session-level traffic source data from GA4.
    Includes sessions, users, and pageviews by source/medium (session-scoped).
    """
    from google_analytics.helpers.data_processing import get_report
    
    end_date = datetime.date.today() - datetime.timedelta(days=1)  # Yesterday
    start_date = end_date - datetime.timedelta(days=days_back)
    
    dimensions = [
        Dimension(name="date"),
        Dimension(name="sessionSource"),
        Dimension(name="sessionMedium"),
        Dimension(name="sessionCampaignName"),
    ]
    
    metrics = [
        Metric(name="sessions"),
        Metric(name="totalUsers"),
        Metric(name="newUsers"),
        Metric(name="screenPageViews"),
    ]
    
    for row in get_report(
        client=client,
        property_id=property_id,
        dimension_list=dimensions,
        metric_list=metrics,
        limit=10000,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
    ):
        yield {
            "property_id": str(property_id),
            "managing_system": group_name,
            "date": row.get("date"),
            "session_source": row.get("sessionSource"),
            "session_medium": row.get("sessionMedium"),
            "session_campaign": row.get("sessionCampaignName"),
            "sessions": int(row.get("sessions_INTEGER", 0)),
            "total_users": int(row.get("totalUsers_INTEGER", 0)),
            "new_users": int(row.get("newUsers_INTEGER", 0)),
            "screen_page_views": int(row.get("screenPageViews_INTEGER", 0)),
        }


@dlt.resource(
    name="ga4_user_engagement",
    primary_key=["property_id", "date"],
    write_disposition="merge",
)
def user_engagement(
    client: BetaAnalyticsDataClient,
    property_id: int,
    group_name: str,
    days_back: int = ROLLING_DAYS,
) -> Iterator[TDataItem]:
    """
    Fetches user engagement metrics from GA4.
    Includes engagement rate, session duration, etc.
    """
    from google_analytics.helpers.data_processing import get_report
    
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    start_date = end_date - datetime.timedelta(days=days_back)
    
    dimensions = [
        Dimension(name="date"),
    ]
    
    metrics = [
        Metric(name="sessions"),
        Metric(name="engagementRate"),
        Metric(name="averageSessionDuration"),
        Metric(name="bounceRate"),
        Metric(name="screenPageViewsPerSession"),
        Metric(name="eventCount"),
    ]
    
    for row in get_report(
        client=client,
        property_id=property_id,
        dimension_list=dimensions,
        metric_list=metrics,
        limit=10000,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
    ):
        yield {
            "property_id": str(property_id),
            "managing_system": group_name,
            "date": row.get("date"),
            "sessions": int(row.get("sessions_INTEGER", 0)),
            "engagement_rate": float(row.get("engagementRate_FLOAT", 0)),
            "average_session_duration": float(row.get("averageSessionDuration_SECONDS", 0)),
            "bounce_rate": float(row.get("bounceRate_FLOAT", 0)),
            "screen_page_views_per_session": float(row.get("screenPageViewsPerSession_FLOAT", 0)),
            "event_count": int(row.get("eventCount_INTEGER", 0)),
        }


@dlt.resource(
    name="ga4_device_category",
    primary_key=["property_id", "date", "device_category"],
    write_disposition="merge",
)
def device_category(
    client: BetaAnalyticsDataClient,
    property_id: int,
    group_name: str,
    days_back: int = ROLLING_DAYS,
) -> Iterator[TDataItem]:
    """
    Fetches device category breakdown from GA4.
    """
    from google_analytics.helpers.data_processing import get_report
    
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    start_date = end_date - datetime.timedelta(days=days_back)
    
    dimensions = [
        Dimension(name="date"),
        Dimension(name="deviceCategory"),
    ]
    
    metrics = [
        Metric(name="sessions"),
        Metric(name="totalUsers"),
        Metric(name="conversions"),
        Metric(name="totalRevenue"),
    ]
    
    for row in get_report(
        client=client,
        property_id=property_id,
        dimension_list=dimensions,
        metric_list=metrics,
        limit=10000,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
    ):
        yield {
            "property_id": str(property_id),
            "managing_system": group_name,
            "date": row.get("date"),
            "device_category": row.get("deviceCategory"),
            "sessions": int(row.get("sessions_INTEGER", 0)),
            "total_users": int(row.get("totalUsers_INTEGER", 0)),
            "conversions": float(row.get("conversions_INTEGER", 0)),
            "total_revenue": float(row.get("totalRevenue_CURRENCY", 0)),
        }


@dlt.resource(
    name="ga4_events",
    primary_key=["property_id", "date", "event_name"],
    write_disposition="merge",
)
def events(
    client: BetaAnalyticsDataClient,
    property_id: int,
    group_name: str,
    days_back: int = ROLLING_DAYS,
) -> Iterator[TDataItem]:
    """
    Fetches event data from GA4.
    """
    from google_analytics.helpers.data_processing import get_report
    
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    start_date = end_date - datetime.timedelta(days=days_back)
    
    dimensions = [
        Dimension(name="date"),
        Dimension(name="eventName"),
    ]
    
    metrics = [
        Metric(name="eventCount"),
        Metric(name="eventCountPerUser"),
        Metric(name="eventValue"),
    ]
    
    for row in get_report(
        client=client,
        property_id=property_id,
        dimension_list=dimensions,
        metric_list=metrics,
        limit=10000,
        start_date=start_date.strftime("%Y-%m-%d"),
        end_date=end_date.strftime("%Y-%m-%d"),
    ):
        yield {
            "property_id": str(property_id),
            "managing_system": group_name,
            "date": row.get("date"),
            "event_name": row.get("eventName"),
            "event_count": int(row.get("eventCount_INTEGER", 0)),
            "event_count_per_user": float(row.get("eventCountPerUser_FLOAT", 0)),
            "event_value": float(row.get("eventValue_CURRENCY", 0)),
        }


# List of all available sources
all_sources = [
    traffic_sources,
    session_traffic_sources,
    user_engagement,
    device_category,
    events,
]
