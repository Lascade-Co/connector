"""
This script will help you obtain an OAuth token from your GCP account with access to GA4. Alternatively service account credentials can be used (see docs)
This script will receive client_id and client_secret to produce an OAuth refresh_token which is then saved in secrets.toml along with client credentials.

Before running this script you must:
1. Ensure your email used for the GCP account has access to the GA4 property.
2. Open a gcp project in your GCP account.
3. Enable the Analytics API in the project
4. Search credentials in the search bar and go to Credentials
5. Create credentials -> OAuth client ID -> Select Desktop App from Application type and give a name to the client.
6. Download the credentials and fill client_id, client_secret and project_id in secrets.toml
7. Go back to credentials and select OAuth consent screen in the left
8. Fill in App name, user support email(your email), authorized domain (localhost.com), dev contact info (your email again)
9. Add the following scope: “https://www.googleapis.com/auth/analytics.readonly”
10. Add your own email as a test user."""
import json

from dlt.sources.credentials import GcpOAuthCredentials


def print_refresh_token() -> None:
    client_id = input("Enter your GCP OAuth client ID: ").strip()
    client_secret = input("Enter your GCP OAuth client secret: ").strip()
    project_id = input("Enter your GCP project ID: ").strip()
    email = input("Enter your email (used for GCP account): ").strip()
    customer_ids = input("Enter your Google Ads customer IDs (comma-separated): ").strip().split(",")
    refresh_token = input("Enter your GCP OAuth refresh token (leave empty to generate a new one): ").strip()

    credentials = GcpOAuthCredentials(
        client_id=client_id,
        client_secret=client_secret,
        project_id=project_id,
    )
    if not refresh_token:
        credentials.auth("https://www.googleapis.com/auth/adwords")

    config = {
        "client_id": credentials.client_id,
        "client_secret": credentials.client_secret,
        "project_id": credentials.project_id,
        "refresh_token": credentials.refresh_token or refresh_token,
        "email": email,
        "accounts_ids": [int(customer_id.replace("-", "").strip()) for customer_id in customer_ids],
    }
    print("Config: ")
    print(json.dumps(config, indent=2))


if __name__ == "__main__":
    print_refresh_token()
