"""
App Store Connect Analytics Sources
Extracts analytics metrics: impressions, downloads, revenue, conversion rates, etc.
"""
import csv
import io
import logging
import os
import zipfile
import gzip
import requests
from typing import Iterator, Dict, Any

import dlt
from dlt.common.typing import TDataItem

from app_store.helpers import AppStoreClient

# Default rolling window for daily runs
ROLLING_DAYS = 7


def get_days_back() -> int:
    """
    Get days_back from environment variable for backfill, or use default.
    
    Returns:
        Number of days to look back (default: 7)
    """
    backfill_days = os.getenv("APPSTORE_BACKFILL_DAYS")
    if backfill_days:
        try:
            days_int = int(backfill_days)
            if days_int > 0:
                return days_int
        except ValueError:
            pass
    return ROLLING_DAYS


def get_existing_analytics_report_request(client: AppStoreClient, app_id: str, app_name: str, is_backfill: bool = False) -> str:
    """
    Get existing analytics report request for an app.
    Does NOT create new requests - only reads existing ones.
    
    Args:
        client: Authenticated AppStoreClient
        app_id: App Store Connect app ID (UUID)
        app_name: Human-readable app name
        is_backfill: If True, looks for ONE_TIME_SNAPSHOT; otherwise ONGOING
        
    Returns:
        Report request ID if found, None otherwise
    """
    try:
        # Determine access type based on backfill mode
        access_type = "ONE_TIME_SNAPSHOT" if is_backfill else "ONGOING"
        
        # Check if report request already exists
        logging.info(f"Looking for existing {access_type} analytics report request for {app_name}")
        response = client.get(f"/v1/apps/{app_id}/analyticsReportRequests", {
            "filter[accessType]": access_type,
            "limit": 1
        })
        
        existing_requests = response.get("data", [])
        if existing_requests:
            request_id = existing_requests[0]["id"]
            logging.info(f"Found existing {access_type} report request: {request_id}")
            return request_id
        
        # No existing request found
        logging.warning(f"No {access_type} report request found for {app_name}. Report requests must be created by an Admin user first.")
        return None
        
    except Exception as e:
        logging.error(f"Error checking for analytics report request for {app_name}: {e}")
        return None


def download_and_parse_report_segment(segment_url: str) -> Iterator[Dict[str, Any]]:
    """
    Download and parse a report segment (CSV/TSV in ZIP format).
    
    Args:
        segment_url: URL to download the segment
        
    Yields:
        Parsed rows as dictionaries
    """
    try:
        response = requests.get(segment_url, timeout=60)
        response.raise_for_status()

        content = response.content
        ctype = response.headers.get("Content-Type", "")
        clen = response.headers.get("Content-Length", "")
        logging.info(f"Segment download content-type={ctype} content-length={clen}")

        # ZIP magic
        if content[:4] == b"PK\x03\x04":
            with zipfile.ZipFile(io.BytesIO(content)) as zip_file:
                file_names = zip_file.namelist()
                if not file_names:
                    logging.warning("Empty ZIP file")
                    return
                data_file = file_names[0]
                with zip_file.open(data_file) as f:
                    text = f.read().decode("utf-8")
        # GZIP magic
        elif content[:2] == b"\x1f\x8b":
            text = gzip.decompress(content).decode("utf-8")
        else:
            # Assume plain text CSV/TSV
            text = content.decode("utf-8")

        first_line = text.split("\n", 1)[0]
        delimiter = "\t" if "\t" in first_line else ","
        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        for row in reader:
            yield row

    except Exception as e:
        logging.error(f"Error downloading/parsing segment: {e}")
        raise


def fetch_analytics_reports(
    client: AppStoreClient,
    app_id: str,
    app_name: str,
    group_name: str,
    category: str,
    is_backfill: bool = False
) -> Iterator[TDataItem]:
    """
    Shared helper to fetch analytics reports for a given category.
    
    Args:
        client: Authenticated AppStoreClient
        app_id: App Store Connect app ID (UUID)
        app_name: Human-readable app name
        group_name: Group identifier for tagging
        category: Report category (APP_STORE_ENGAGEMENT, APP_STORE_COMMERCE, APP_USAGE)
        is_backfill: If True, uses ONE_TIME_SNAPSHOT for historical data
        
    Yields:
        Parsed report rows with metadata
    """
    try:
        # Get existing report request (does not create new ones)
        report_request_id = get_existing_analytics_report_request(client, app_id, app_name, is_backfill)
        
        # Skip if no report request exists
        if not report_request_id:
            logging.info(f"Skipping {category} for {app_name} - no report request found. An Admin user must create report requests first.")
            return
        
        # Get available reports filtered by category
        reports_response = client.get(
            f"/v1/analyticsReportRequests/{report_request_id}/reports",
            {"filter[category]": category, "limit": 200}
        )
        
        reports = reports_response.get("data", [])
        if not reports:
            logging.info(f"No {category} reports available yet for {app_name}")
            return
        
        # Process each report
        for report in reports:
            report_id = report["id"]
            report_name = report.get("attributes", {}).get("name", "Unknown")
            
            logging.info(f"Processing report: {report_name}")
            
            # Get report instances (daily/weekly/monthly granularity)
            instances_response = client.get(f"/v1/analyticsReports/{report_id}/instances", {"limit": 10})
            instances = instances_response.get("data", [])
            
            for instance in instances:
                instance_id = instance["id"]
                granularity = instance.get("attributes", {}).get("granularity", "DAILY")
                
                # Get segments (download URLs)
                segments_response = client.get(
                    f"/v1/analyticsReportInstances/{instance_id}/segments",
                    {"fields[analyticsReportSegments]": "url,checksum,sizeInBytes"}
                )
                
                segments = segments_response.get("data", [])
                for segment in segments:
                    segment_url = segment.get("attributes", {}).get("url")
                    if not segment_url:
                        continue
                    
                    # Download and parse segment data
                    for row in download_and_parse_report_segment(segment_url):
                        # Enrich with metadata
                        row["_app_id"] = app_id
                        row["_app_name"] = app_name
                        row["_group_name"] = group_name
                        row["_report_name"] = report_name
                        row["_granularity"] = granularity
                        yield row
                        
    except Exception as e:
        body = getattr(getattr(e, "response", None), "text", "")
        logging.error(f"Error fetching {category} for {app_name}: {e} {body}")


@dlt.resource(name="app_store_engagement", write_disposition="append")
def app_store_engagement(
    client: AppStoreClient,
    app_id: str,
    app_name: str,
    group_name: str,
    is_backfill: bool = False
) -> Iterator[TDataItem]:
    """
    Extract App Store Engagement metrics: Impressions, Product Page Views, Conversion Rate.
    
    Metrics included:
    - Impressions
    - Product Page Views
    - Total Downloads
    - Conversion Rate
    - App Units (downloads by source)
    """
    logging.info(f"Fetching App Store Engagement metrics for {app_name}")
    return fetch_analytics_reports(client, app_id, app_name, group_name, "APP_STORE_ENGAGEMENT", is_backfill)


@dlt.resource(name="app_store_commerce", write_disposition="append")
def app_store_commerce(
    client: AppStoreClient,
    app_id: str,
    app_name: str,
    group_name: str,
    is_backfill: bool = False
) -> Iterator[TDataItem]:
    """
    Extract App Store Commerce metrics: Revenue, Sales, Paying Users.
    
    Metrics included:
    - Total Revenue
    - Paying Users
    - Paying Sessions
    - Revenue Per Paying User
    - In-App Purchases
    """
    logging.info(f"Fetching App Store Commerce metrics for {app_name}")
    return fetch_analytics_reports(client, app_id, app_name, group_name, "COMMERCE", is_backfill)


@dlt.resource(name="app_usage", write_disposition="append")
def app_usage(
    client: AppStoreClient,
    app_id: str,
    app_name: str,
    group_name: str,
    is_backfill: bool = False
) -> Iterator[TDataItem]:
    """
    Extract App Usage metrics: Active Devices, Installs, Sessions, Crashes.
    
    Metrics included:
    - Active Devices
    - Installations
    - Deletions (Uninstalls)
    - Sessions
    - Active Last 30 Days
    - Crashes
    """
    logging.info(f"Fetching App Usage metrics for {app_name}")
    return fetch_analytics_reports(client, app_id, app_name, group_name, "APP_USAGE", is_backfill)


# List of all analytics sources
all_sources = [
    app_store_engagement,
    app_store_commerce,
    app_usage,
]
