"""
One-time script to create Analytics Report Requests for all apps.
This must be run with an Admin API key.

Usage:
    python pipelines/app_store/create_report_requests.py <group_name>

This creates ONGOING report requests for all apps in the group.
After running this once, the regular pipeline can read the reports with Marketing/Finance keys.
"""
import sys
import logging
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app_store.helpers import get_app_store_client
from utils import get_for_group

logging.basicConfig(level=logging.INFO, format="%(levelname)s │ %(message)s")


def create_report_requests_for_group(group_name: str):
    """
    Create ONGOING analytics report requests for all apps in a group.
    Requires Admin API key.
    """
    logging.info(f"Creating analytics report requests for group: {group_name}")
    
    # Get group config and apps
    group, apps = get_for_group(group_name, "app_store")
    
    # Create API client
    private_key = group.get("private_key")
    if not private_key:
        raise ValueError("Private key not found in config")
    
    client = get_app_store_client(
        key_id=group["key_id"],
        issuer_id=group["issuer_id"],
        private_key=private_key
    )
    
    # Fetch accessible apps
    logging.info("Fetching list of accessible apps...")
    apps_by_bundle = {}
    for app in client.get_paginated("/v1/apps", params={"limit": 200}):
        app_id = app.get("id")
        bundle_id = app.get("attributes", {}).get("bundleId")
        if app_id and bundle_id:
            apps_by_bundle[bundle_id] = app_id
    
    logging.info(f"Found {len(apps_by_bundle)} accessible apps")
    
    # Create report requests for each app
    created_count = 0
    existing_count = 0
    failed_count = 0
    
    for app_config in apps:
        app_name = app_config.get("app_name")
        bundle_id = app_config.get("bundle_id")
        
        app_id = apps_by_bundle.get(bundle_id)
        if not app_id:
            logging.warning(f"Could not find app ID for {app_name} ({bundle_id})")
            failed_count += 1
            continue
        
        logging.info(f"\nProcessing: {app_name} ({app_id})")
        
        try:
            # Check if ONGOING report request already exists
            response = client.get(f"/v1/apps/{app_id}/analyticsReportRequests", {
                "filter[accessType]": "ONGOING",
                "limit": 1
            })
            
            existing_requests = response.get("data", [])
            if existing_requests:
                request_id = existing_requests[0]["id"]
                logging.info(f"  ✓ ONGOING report request already exists: {request_id}")
                existing_count += 1
                continue
            
            # Create new ONGOING report request
            logging.info(f"  Creating ONGOING report request...")
            payload = {
                "data": {
                    "type": "analyticsReportRequests",
                    "attributes": {
                        "accessType": "ONGOING"
                    },
                    "relationships": {
                        "app": {
                            "data": {
                                "type": "apps",
                                "id": app_id
                            }
                        }
                    }
                }
            }
            
            response = client.post("/v1/analyticsReportRequests", payload)
            request_id = response["data"]["id"]
            logging.info(f"  ✓ Created ONGOING report request: {request_id}")
            created_count += 1
            
        except Exception as e:
            logging.error(f"  ✗ Error for {app_name}: {e}")
            failed_count += 1
    
    # Summary
    logging.info(f"\n{'='*60}")
    logging.info(f"Summary:")
    logging.info(f"  Created: {created_count}")
    logging.info(f"  Already existed: {existing_count}")
    logging.info(f"  Failed: {failed_count}")
    logging.info(f"{'='*60}")
    
    if created_count > 0:
        logging.info(f"\n⏰ Note: New report requests take 1-2 days to generate initial data.")
        logging.info(f"   After that, the regular pipeline can read the reports with Marketing/Finance keys.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python pipelines/app_store/create_report_requests.py <group_name>")
        print("\nExample: python pipelines/app_store/create_report_requests.py d1")
        print("\nNote: This requires an Admin API key in secrets/app_store.json")
        sys.exit(1)
    
    group_name = sys.argv[1]
    create_report_requests_for_group(group_name)
