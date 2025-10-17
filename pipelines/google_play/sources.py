"""
Google Play Console data sources using dlt.
Extracts data from Google Cloud Storage exports.
"""
import logging
import os
from typing import Iterator, Dict, Any

import dlt
from dlt.common.typing import TDataItem

from pipelines.google_play.storage import (
    get_storage_client,
    generate_year_months,
    get_installs_files,
    get_crashes_files,
    get_ratings_files,
    download_csv_from_gcs
)
from utils import date_key_from_play

ROLLING_MONTHS = 3


def extract_dimension_type(blob_path: str) -> str:
    """
    Extract dimension type from blob path filename.
    
    Args:
        blob_path: GCS blob path (e.g., 'stats/installs/installs_com.app_202410_country.csv')
    
    Returns:
        Dimension type (e.g., 'country', 'device', 'app_version', etc.)
    """
    if "_country.csv" in blob_path:
        return "country"
    elif "_device.csv" in blob_path:
        return "device"
    elif "_app_version.csv" in blob_path:
        return "app_version"
    elif "_carrier.csv" in blob_path:
        return "carrier"
    elif "_language.csv" in blob_path:
        return "language"
    elif "_os_version.csv" in blob_path:
        return "os_version"
    elif "_traffic_source.csv" in blob_path:
        return "traffic_source"
    else:
        return "overview"


def get_months_back() -> int:
    """Get months_back from environment variable for backfill, or use default."""
    backfill_months = os.getenv("GOOGLE_PLAY_BACKFILL_MONTHS")
    if backfill_months:
        try:
            months_int = int(backfill_months)
        except ValueError:
            logging.warning(
                f"Invalid GOOGLE_PLAY_BACKFILL_MONTHS value: {backfill_months}. Using default: {ROLLING_MONTHS}")
            return ROLLING_MONTHS

        if months_int > 0:
            return months_int
        else:
            logging.warning(
                f"GOOGLE_PLAY_BACKFILL_MONTHS must be positive. Got: {months_int}. Using default: {ROLLING_MONTHS}")
    return ROLLING_MONTHS


@dlt.resource(
    name="google_play_installs",
    primary_key=["date", "package_name", "dimension_type", "dimension_value"],
    write_disposition="merge",
)
def play_installs(
        credentials_path: str,
        bucket_name: str,
        package_name: str,
        app_name: str,
        months_back: int = ROLLING_MONTHS
) -> Iterator[TDataItem]:
    """
    Fetch install statistics from Google Play Console via GCS exports.
    
    Args:
        credentials_path: Path to service account JSON key
        bucket_name: GCS bucket name (e.g., 'pubsite_prod_rev_123456')
        package_name: Android package name (e.g., 'com.example.app')
        app_name: Friendly app name for tracking
        months_back: Number of months to backfill
        
    Yields:
        Dictionary with install metrics for each date/dimension combination
    """
    client = get_storage_client(credentials_path)
    year_months = generate_year_months(months_back)

    logging.info(f"Fetching install data for {package_name} ({app_name})")
    logging.info(f"Months to fetch: {', '.join(year_months)}")

    blob_paths = get_installs_files(client, bucket_name, package_name, year_months)
    logging.info(f"Found {len(blob_paths)} install files")

    for blob_path in blob_paths:
        dimension_type = extract_dimension_type(blob_path)
        logging.info(f"Processing: {blob_path}")

        for row in download_csv_from_gcs(client, bucket_name, blob_path):
            # CSV columns (may vary by dimension):
            # Date, Package Name, Daily Device Installs, Daily Device Uninstalls, 
            # Daily Device Upgrades, Total User Installs, Daily User Installs, 
            # Daily User Uninstalls, Active Device Installs, Install events, 
            # Update events, Uninstall events, [Dimension Column]

            dimension_value = next(
                (row.get(key) for key in
                 ['Country', 'Device', 'App Version Code', 'Carrier', 'Language', 'Android OS Version'] if
                 row.get(key)),
                'all'
            )

            yield {
                "date": row.get('Date'),
                "date_key_pt": date_key_from_play(row.get('Date')),
                "package_name": package_name,
                "app_name": app_name,
                "dimension_type": dimension_type,
                "dimension_value": dimension_value,
                # Install metrics
                "daily_device_installs": int(row.get('Daily Device Installs', 0) or 0),
                "daily_device_uninstalls": int(row.get('Daily Device Uninstalls', 0) or 0),
                "daily_device_upgrades": int(row.get('Daily Device Upgrades', 0) or 0),
                "total_user_installs": int(row.get('Total User Installs', 0) or 0),
                "daily_user_installs": int(row.get('Daily User Installs', 0) or 0),
                "daily_user_uninstalls": int(row.get('Daily User Uninstalls', 0) or 0),
                "active_device_installs": int(row.get('Active Device Installs', 0) or 0),
                "install_events": int(row.get('Install events', 0) or 0),
                "update_events": int(row.get('Update events', 0) or 0),
                "uninstall_events": int(row.get('Uninstall events', 0) or 0),
            }


@dlt.resource(
    name="google_play_crashes",
    primary_key=["date", "package_name", "dimension_type", "dimension_value"],
    write_disposition="merge",
)
def play_crashes(
        credentials_path: str,
        bucket_name: str,
        package_name: str,
        app_name: str,
        months_back: int = ROLLING_MONTHS
) -> Iterator[TDataItem]:
    """
    Fetch crash statistics from Google Play Console via GCS exports.
    
    Args:
        credentials_path: Path to service account JSON key
        bucket_name: GCS bucket name (e.g., 'pubsite_prod_rev_123456')
        package_name: Android package name (e.g., 'com.example.app')
        app_name: Friendly app name for tracking
        months_back: Number of months to backfill
        
    Yields:
        Dictionary with crash metrics for each date/dimension combination
    """
    client = get_storage_client(credentials_path)
    year_months = generate_year_months(months_back)

    logging.info(f"Fetching crash data for {package_name} ({app_name})")
    logging.info(f"Months to fetch: {', '.join(year_months)}")

    blob_paths = get_crashes_files(client, bucket_name, package_name, year_months)
    logging.info(f"Found {len(blob_paths)} crash files")

    for blob_path in blob_paths:
        dimension_type = extract_dimension_type(blob_path)
        logging.info(f"Processing: {blob_path}")

        for row in download_csv_from_gcs(client, bucket_name, blob_path):
            # CSV columns (may vary by dimension):
            # Date, Package Name, Daily Crashes, Daily ANRs, [Dimension Column]

            dimension_value = next(
                (row.get(key) for key in ['Device', 'App Version Code', 'Android OS Version'] if row.get(key)),
                'all'
            )

            yield {
                "date": row.get('Date'),
                "date_key_pt": date_key_from_play(row.get('Date')),
                "package_name": package_name,
                "app_name": app_name,
                "dimension_type": dimension_type,
                "dimension_value": dimension_value,
                # Crash metrics
                "daily_crashes": int(row.get('Daily Crashes', 0) or 0),
                "daily_anrs": int(row.get('Daily ANRs', 0) or 0),
            }


@dlt.resource(
    name="google_play_ratings",
    primary_key=["date", "package_name", "dimension_type", "dimension_value"],
    write_disposition="merge",
)
def play_ratings(
        credentials_path: str,
        bucket_name: str,
        package_name: str,
        app_name: str,
        months_back: int = ROLLING_MONTHS
) -> Iterator[TDataItem]:
    """
    Fetch ratings statistics from Google Play Console via GCS exports.
    
    Args:
        credentials_path: Path to service account JSON key
        bucket_name: GCS bucket name (e.g., 'pubsite_prod_rev_123456')
        package_name: Android package name (e.g., 'com.example.app')
        app_name: Friendly app name for tracking
        months_back: Number of months to backfill
        
    Yields:
        Dictionary with rating metrics for each date/dimension combination
    """
    client = get_storage_client(credentials_path)
    year_months = generate_year_months(months_back)

    logging.info(f"Fetching ratings data for {package_name} ({app_name})")
    logging.info(f"Months to fetch: {', '.join(year_months)}")

    blob_paths = get_ratings_files(client, bucket_name, package_name, year_months)
    logging.info(f"Found {len(blob_paths)} ratings files")

    for blob_path in blob_paths:
        dimension_type = extract_dimension_type(blob_path)
        logging.info(f"Processing: {blob_path}")

        for row in download_csv_from_gcs(client, bucket_name, blob_path):
            # CSV columns (may vary by dimension):
            # Date, Package Name, Daily Average Rating, Total Average Rating, [Dimension Column]

            dimension_value = next(
                (row.get(key) for key in
                 ['Country', 'Device', 'App Version Code', 'Carrier', 'Language', 'Android OS Version'] if
                 row.get(key)),
                'all'
            )

            yield {
                "date": row.get('Date'),
                "date_key_pt": date_key_from_play(row.get('Date')),
                "package_name": package_name,
                "app_name": app_name,
                "dimension_type": dimension_type,
                "dimension_value": dimension_value,
                # Rating metrics
                "daily_average_rating": float(row.get('Daily Average Rating', 0) or 0),
                "total_average_rating": float(row.get('Total Average Rating', 0) or 0),
            }


@dlt.resource(
    name="google_play_store_performance",
    primary_key=["date", "package_name", "dimension_type", "dimension_value"],
    write_disposition="merge",
)
def play_store_performance(
        credentials_path: str,
        bucket_name: str,
        package_name: str,
        app_name: str,
        months_back: int = ROLLING_MONTHS,
) -> Iterator[Dict[str, Any]]:
    """
    Extract store listing performance data from Google Cloud Storage.
    Includes store listing visitors, acquisitions, and conversion rates.
    
    Args:
        credentials_path: Path to service account JSON
        bucket_name: GCS bucket name
        package_name: Android package name
        app_name: Friendly app name
        months_back: Number of months to backfill (0 = current month only)
    
    Yields:
        Store performance records
    """
    from pipelines.google_play.storage import get_storage_client, download_csv_from_gcs, generate_year_months, \
        get_store_performance_files

    client = get_storage_client(credentials_path)
    year_months = generate_year_months(months_back)

    logging.info(f"Fetching store performance data for {package_name} ({app_name})")
    logging.info(f"Months to fetch: {', '.join(year_months)}")

    blob_paths = get_store_performance_files(client, bucket_name, package_name, year_months)
    logging.info(f"Found {len(blob_paths)} store performance files")

    for blob_path in blob_paths:
        dimension_type = extract_dimension_type(blob_path)
        logging.info(f"Processing: {blob_path}")

        for row in download_csv_from_gcs(client, bucket_name, blob_path):
            # CSV columns (vary by dimension):
            # country: Date, Package name, Country / region, Store listing acquisitions, Store listing visitors, Store listing conversion rate
            # traffic_source: Date, Package name, Traffic source, Search term, UTM source, UTM campaign, Store listing acquisitions, Store listing visitors, Store listing conversion rate

            if dimension_type == "country":
                dimension_value = row.get('Country / region', 'all')
                traffic_source = None
                search_term = None
                utm_source = None
                utm_campaign = None
            elif dimension_type == "traffic_source":
                dimension_value = row.get('Traffic source', 'all')
                traffic_source = row.get('Traffic source')
                search_term = row.get('Search term')
                utm_source = row.get('UTM source')
                utm_campaign = row.get('UTM campaign')
            else:
                dimension_value = 'all'
                traffic_source = None
                search_term = None
                utm_source = None
                utm_campaign = None

            yield {
                "date": row.get('Date'),
                "date_key_pt": date_key_from_play(row.get('Date')),
                "package_name": package_name,
                "app_name": app_name,
                "dimension_type": dimension_type,
                "dimension_value": dimension_value,
                # Store performance metrics
                "store_listing_acquisitions": int(row.get('Store listing acquisitions', 0) or 0),
                "store_listing_visitors": int(row.get('Store listing visitors', 0) or 0),
                "store_listing_conversion_rate": float(row.get('Store listing conversion rate', 0) or 0),
                # Traffic source details (only for traffic_source dimension)
                "traffic_source": traffic_source,
                "search_term": search_term,
                "utm_source": utm_source,
                "utm_campaign": utm_campaign,
            }


# List of all available sources
all_sources = [
    play_installs,
    play_crashes,
    play_ratings,
    play_store_performance,
]
