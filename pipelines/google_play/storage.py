"""
Google Cloud Storage utilities for downloading Google Play Console reports.
"""
import csv
import io
import logging
from datetime import datetime
from typing import Iterator, List, Dict, Any

from google.cloud import storage
from google.oauth2 import service_account


def generate_year_months(months_back: int) -> List[str]:
    """
    Generate a list of year-month strings (YYYYMM format) for the backfill period.
    
    Args:
        months_back: Number of months to go back from current month
        
    Returns:
        List of strings in YYYYMM format, e.g., ['202410', '202409', '202408']
    """
    year_months = []
    current = datetime.now()
    
    for i in range(months_back + 1):  # +1 to include current month
        # Calculate the target month
        target_month = current.month - i
        target_year = current.year
        
        # Handle year rollover
        while target_month <= 0:
            target_month += 12
            target_year -= 1
        
        year_months.append(f"{target_year}{target_month:02d}")
    
    return sorted(year_months)  # Return in chronological order


def get_storage_client(credentials_path: str) -> storage.Client:
    """
    Create and return a Google Cloud Storage client.
    
    Args:
        credentials_path: Path to the service account JSON key file
        
    Returns:
        Authenticated storage.Client instance
    """
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    return storage.Client(credentials=credentials, project=credentials.project_id)


def download_csv_from_gcs(
    client: storage.Client,
    bucket_name: str,
    blob_path: str
) -> Iterator[Dict[str, Any]]:
    """
    Download a CSV file from Google Cloud Storage and yield rows as dictionaries.
    
    Args:
        client: Authenticated storage.Client instance
        bucket_name: Name of the GCS bucket (e.g., 'pubsite_prod_rev_123456')
        blob_path: Path to the blob within the bucket
        
    Yields:
        Dictionary for each row in the CSV
    """
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
        if not blob.exists():
            logging.warning(f"Blob does not exist: {blob_path}")
            return
        
        # Download as bytes and decode from UTF-16 (Google Play uses UTF-16)
        content_bytes = blob.download_as_bytes()
        content_text = content_bytes.decode('utf-16')
        
        # Parse CSV
        csv_reader = csv.DictReader(io.StringIO(content_text))
        
        for row in csv_reader:
            yield row
            
    except Exception as e:
        logging.error(f"Error downloading {blob_path}: {e}")


def list_blobs_with_prefix(
    client: storage.Client,
    bucket_name: str,
    prefix: str
) -> List[str]:
    """
    List all blobs in a bucket with a given prefix.
    
    Args:
        client: Authenticated storage.Client instance
        bucket_name: Name of the GCS bucket
        prefix: Prefix to filter blobs (e.g., 'stats/installs/')
        
    Returns:
        List of blob names (paths)
    """
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs]


def get_stats_files(
    client: storage.Client,
    bucket_name: str,
    package_name: str,
    year_months: List[str],
    stats_type: str
) -> List[str]:
    """
    Generic function to get statistics files for specified months.
    
    Args:
        client: Authenticated storage.Client instance
        bucket_name: Name of the GCS bucket
        package_name: Android package name (e.g., 'com.example.app')
        year_months: List of year-month strings (e.g., ['202410', '202409'])
        stats_type: Type of stats (e.g., 'installs', 'crashes', 'ratings', 'store_performance')
        
    Returns:
        List of blob paths matching the criteria
    """
    prefix = f"stats/{stats_type}/{stats_type}_{package_name}_"
    all_blobs = list_blobs_with_prefix(client, bucket_name, prefix)
    
    # Filter for specific months
    matching_blobs = []
    for blob_path in all_blobs:
        for ym in year_months:
            if f"_{ym}_" in blob_path or f"_{ym}." in blob_path:
                matching_blobs.append(blob_path)
                break
    
    return matching_blobs


def get_installs_files(
    client: storage.Client,
    bucket_name: str,
    package_name: str,
    year_months: List[str]
) -> List[str]:
    """Get list of install statistics files for specified months."""
    return get_stats_files(client, bucket_name, package_name, year_months, 'installs')


def get_crashes_files(
    client: storage.Client,
    bucket_name: str,
    package_name: str,
    year_months: List[str]
) -> List[str]:
    """Get list of crash statistics files for specified months."""
    return get_stats_files(client, bucket_name, package_name, year_months, 'crashes')


def get_ratings_files(
    client: storage.Client,
    bucket_name: str,
    package_name: str,
    year_months: List[str]
) -> List[str]:
    """Get list of ratings CSV files for a package across specified months."""
    return get_stats_files(client, bucket_name, package_name, year_months, 'ratings')


def get_store_performance_files(
    client: storage.Client,
    bucket_name: str,
    package_name: str,
    year_months: List[str]
) -> List[str]:
    """Get list of store performance CSV files for a package across specified months."""
    return get_stats_files(client, bucket_name, package_name, year_months, 'store_performance')


