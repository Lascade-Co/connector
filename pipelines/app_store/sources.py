"""
App Store Connect data sources for dlt pipeline.
"""
import os
import logging
from typing import Iterator, Dict, Any, List
from datetime import datetime, timedelta

import dlt
from dlt.sources.helpers import requests

from app_store.helpers import AppStoreClient


def get_days_back() -> int:
    """
    Get the number of days to look back for data.
    Checks APPSTORE_BACKFILL_DAYS environment variable.
    
    Returns:
        Number of days to look back (default: 7)
    """
    backfill_days = os.getenv("APPSTORE_BACKFILL_DAYS")
    if backfill_days:
        try:
            return int(backfill_days)
        except ValueError:
            logging.warning(f"Invalid APPSTORE_BACKFILL_DAYS value: {backfill_days}, using default 7")
            return 7
    return 7


@dlt.source
def apps_metadata(
    client: AppStoreClient,
    group_name: str,
    app_ids: List[str] = None
) -> Iterator[dlt.Resource]:
    """
    Extract app metadata from App Store Connect.
    
    Args:
        client: Authenticated AppStoreClient
        group_name: Group identifier for tagging
        app_ids: Optional list of specific app IDs to fetch
    
    Yields:
        dlt.Resource for apps data
    """
    
    @dlt.resource(name="apps", write_disposition="merge", primary_key="id")
    def get_apps():
        """Fetch all apps or specific apps."""
        logging.info(f"Fetching apps metadata for group: {group_name}")
        
        if app_ids:
            # Fetch specific apps
            for app_id in app_ids:
                try:
                    data = client.get(f"/v1/apps/{app_id}")
                    app = data.get("data", {})
                    if app:
                        app["_group_name"] = group_name
                        app["_loaded_at"] = datetime.utcnow().isoformat()
                        yield app
                except Exception as e:
                    logging.error(f"Error fetching app {app_id}: {e}")
        else:
            # Fetch all apps
            for app in client.get_paginated("/v1/apps"):
                app["_group_name"] = group_name
                app["_loaded_at"] = datetime.utcnow().isoformat()
                yield app
    
    yield get_apps


@dlt.source
def app_info(
    client: AppStoreClient,
    app_id: str,
    app_name: str,
    group_name: str
) -> Iterator[dlt.Resource]:
    """
    Extract detailed app information including localizations.
    
    Args:
        client: Authenticated AppStoreClient
        app_id: App Store app ID
        app_name: Human-readable app name
        group_name: Group identifier for tagging
    
    Yields:
        dlt.Resource for app info data
    """
    
    @dlt.resource(name="app_info", write_disposition="merge", primary_key="id")
    def get_app_info():
        """Fetch app info with localizations."""
        logging.info(f"Fetching app info for: {app_name} ({app_id})")
        
        try:
            # Get app info
            params = {
                "include": "appInfoLocalizations,primaryCategory,primarySubcategoryOne,primarySubcategoryTwo"
            }
            data = client.get(f"/v1/apps/{app_id}/appInfos", params)
            
            for info in data.get("data", []):
                info["_app_id"] = app_id
                info["_app_name"] = app_name
                info["_group_name"] = group_name
                info["_loaded_at"] = datetime.utcnow().isoformat()
                yield info
                
        except Exception as e:
            logging.error(f"Error fetching app info for {app_name}: {e}")
    
    yield get_app_info


@dlt.source
def customer_reviews(
    client: AppStoreClient,
    app_id: str,
    app_name: str,
    group_name: str,
    days_back: int = 7
) -> Iterator[dlt.Resource]:
    """
    Extract customer reviews for an app.
    
    Args:
        client: Authenticated AppStoreClient
        app_id: App Store app ID
        app_name: Human-readable app name
        group_name: Group identifier for tagging
        days_back: Number of days to look back
    
    Yields:
        dlt.Resource for customer reviews
    """
    
    @dlt.resource(name="customer_reviews", write_disposition="append")
    def get_reviews():
        """Fetch customer reviews."""
        logging.info(f"Fetching reviews for: {app_name} ({app_id}), last {days_back} days")
        
        try:
            # Calculate date filter
            start_date = (datetime.utcnow() - timedelta(days=days_back)).isoformat()
            
            params = {
                "filter[territory]": "USA",  # Can be parameterized
                "sort": "-createdDate",
                "limit": 200
            }
            
            for review in client.get_paginated(f"/v1/apps/{app_id}/customerReviews", params):
                # Filter by date
                created_date = review.get("attributes", {}).get("createdDate", "")
                if created_date >= start_date:
                    review["_app_id"] = app_id
                    review["_app_name"] = app_name
                    review["_group_name"] = group_name
                    review["_loaded_at"] = datetime.utcnow().isoformat()
                    yield review
                    
        except Exception as e:
            logging.error(f"Error fetching reviews for {app_name}: {e}")
    
    yield get_reviews


@dlt.source
def app_store_versions(
    client: AppStoreClient,
    app_id: str,
    app_name: str,
    group_name: str
) -> Iterator[dlt.Resource]:
    """
    Extract app store versions and build information.
    
    Args:
        client: Authenticated AppStoreClient
        app_id: App Store app ID
        app_name: Human-readable app name
        group_name: Group identifier for tagging
    
    Yields:
        dlt.Resource for app versions
    """
    
    @dlt.resource(name="app_store_versions", write_disposition="merge", primary_key="id")
    def get_versions():
        """Fetch app store versions."""
        logging.info(f"Fetching versions for: {app_name} ({app_id})")
        
        try:
            params = {
                "include": "build,appStoreVersionLocalizations",
                "limit": 200
            }
            
            for version in client.get_paginated(f"/v1/apps/{app_id}/appStoreVersions", params):
                version["_app_id"] = app_id
                version["_app_name"] = app_name
                version["_group_name"] = group_name
                version["_loaded_at"] = datetime.utcnow().isoformat()
                yield version
                
        except Exception as e:
            logging.error(f"Error fetching versions for {app_name}: {e}")
    
    yield get_versions


@dlt.source
def builds(
    client: AppStoreClient,
    app_id: str,
    app_name: str,
    group_name: str,
    days_back: int = 30
) -> Iterator[dlt.Resource]:
    """
    Extract build information for an app.
    
    Args:
        client: Authenticated AppStoreClient
        app_id: App Store app ID
        app_name: Human-readable app name
        group_name: Group identifier for tagging
        days_back: Number of days to look back
    
    Yields:
        dlt.Resource for builds
    """
    
    @dlt.resource(name="builds", write_disposition="merge", primary_key="id")
    def get_builds():
        """Fetch builds."""
        logging.info(f"Fetching builds for: {app_name} ({app_id}), last {days_back} days")
        
        try:
            params = {
                "sort": "-uploadedDate",
                "limit": 200
            }
            
            for build in client.get_paginated(f"/v1/apps/{app_id}/builds", params):
                build["_app_id"] = app_id
                build["_app_name"] = app_name
                build["_group_name"] = group_name
                build["_loaded_at"] = datetime.utcnow().isoformat()
                yield build
                
        except Exception as e:
            logging.error(f"Error fetching builds for {app_name}: {e}")
    
    yield get_builds


@dlt.source
def in_app_purchases(
    client: AppStoreClient,
    app_id: str,
    app_name: str,
    group_name: str
) -> Iterator[dlt.Resource]:
    """
    Extract in-app purchase information.
    
    Args:
        client: Authenticated AppStoreClient
        app_id: App Store app ID
        app_name: Human-readable app name
        group_name: Group identifier for tagging
    
    Yields:
        dlt.Resource for in-app purchases
    """
    
    @dlt.resource(name="in_app_purchases", write_disposition="merge", primary_key="id")
    def get_iaps():
        """Fetch in-app purchases."""
        logging.info(f"Fetching IAPs for: {app_name} ({app_id})")
        
        try:
            params = {
                "include": "iapPriceSchedule",
                "limit": 200
            }
            
            for iap in client.get_paginated(f"/v1/apps/{app_id}/inAppPurchases", params):
                iap["_app_id"] = app_id
                iap["_app_name"] = app_name
                iap["_group_name"] = group_name
                iap["_loaded_at"] = datetime.utcnow().isoformat()
                yield iap
                
        except Exception as e:
            logging.error(f"Error fetching IAPs for {app_name}: {e}")
    
    yield get_iaps


@dlt.source
def beta_testers(
    client: AppStoreClient,
    app_id: str,
    app_name: str,
    group_name: str
) -> Iterator[dlt.Resource]:
    """
    Extract beta tester information (TestFlight).
    
    Args:
        client: Authenticated AppStoreClient
        app_id: App Store app ID
        app_name: Human-readable app name
        group_name: Group identifier for tagging
    
    Yields:
        dlt.Resource for beta testers
    """
    
    @dlt.resource(name="beta_testers", write_disposition="merge", primary_key="id")
    def get_beta_testers():
        """Fetch beta testers."""
        logging.info(f"Fetching beta testers for: {app_name} ({app_id})")
        
        try:
            params = {"limit": 200}
            
            for tester in client.get_paginated(f"/v1/apps/{app_id}/betaTesters", params):
                tester["_app_id"] = app_id
                tester["_app_name"] = app_name
                tester["_group_name"] = group_name
                tester["_loaded_at"] = datetime.utcnow().isoformat()
                yield tester
                
        except Exception as e:
            logging.error(f"Error fetching beta testers for {app_name}: {e}")
    
    yield get_beta_testers


# List of all source functions to run
all_sources = [
    app_info,
    customer_reviews,
    app_store_versions,
    builds,
    in_app_purchases,
    beta_testers,
]
