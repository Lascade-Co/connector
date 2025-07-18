import json
import os, datetime, dlt
import sys

from dlt.common.typing import TDataItem
from dlt.sources.credentials import GcpOAuthCredentials
from google.ads.googleads.client import GoogleAdsClient
from typing import Iterator, List

from google_ads import get_client
from pipelines.google.queries import AD_METRICS_QUERY, CREATIVES_QUERY

ROLLING_DAYS = 30

# ---------- helpers ---------------------------------------------------------

def run_query(
        client: GoogleAdsClient,
        customer_id: str,
        query: str,
):
    ga_service = client.get_service("GoogleAdsService")
    stream = ga_service.search_stream(customer_id=customer_id, query=query)
    for batch in stream:
        for row in batch.results:
            yield row

# ---------- dlt resources ---------------------------------------------------

@dlt.resource(
    name="google_ads",                       # final table name
    primary_key=["account_id", "ad_id", "date"],
    write_disposition="merge",
)
def ads_metrics(client: GoogleAdsClient, customer_ids: List[str], days_back: int = ROLLING_DAYS) -> Iterator[TDataItem]:
    end = datetime.date.today()
    start = end - datetime.timedelta(days=days_back)
    query = AD_METRICS_QUERY.format(start=start, end=end)

    for customer_id in customer_ids:
        for r in run_query(client, customer_id, query):
            m = r.metrics
            yield {
                "date": r.segments.date.value,
                "account_id": customer_id,
                "campaign_id": r.campaign.id,
                "campaign_name": r.campaign.name,
                "ad_group_id": r.ad_group.id,
                "ad_group_name": r.ad_group.name,
                "ad_id": r.ad_group_ad.ad.id,
                "ad_name": r.ad_group_ad.ad.name,
                "impressions": m.impressions,
                "clicks": m.clicks,
                "ctr": m.ctr,
                "average_cpc": m.average_cpc,
                "cpm": m.cost_per_thousand_impressions,
                "spend_micros": m.cost_micros,
                "conversion_value": m.conversions_value,
                "roas": m.conversions_value_per_cost,
                "reach": m.reach,
            }


@dlt.resource(
    name="google_creatives",
    primary_key="ad_id",
    write_disposition="merge",
)
def creatives(
        client: GoogleAdsClient,
        accounts: List[str],
) -> Iterator[TDataItem]:
    for acc in accounts:
        for r in run_query(client, acc, CREATIVES_QUERY):
            ad = r.ad_group_ad.ad
            yield {
                "account_id": acc,
                "ad_id": ad.id,
                "ad_name": ad.name,
                "ad_type": ad.type.name,
                "final_urls": list(ad.final_urls),
            }

def run():
    if len(sys.argv) < 2 or not sys.argv[2]:
        raise ValueError("Please provide a group name as the second argument.")

    group_name = sys.argv[2]
    group = json.load(open("google_accounts.json", "r"))[group_name]

    client = get_client(
        GcpOAuthCredentials(
        client_id=os.getenv("GADS_CLIENT_ID_A"),
        client_secret=os.getenv("GADS_CLIENT_SECRET_A"),
        refresh_token=os.getenv("GADS_REFRESH_TOKEN_A"),
        project_id="my-gcp-proj")
    )

    pipe = dlt.pipeline(
        pipeline_name=f"google_ads_{group_name}",
        destination="clickhouse",
        dataset_name="google",               # -> tables google.google_ads / google.google_creatives
    )
    pipe.run([
        ads_metrics(client, dev_token),    # bound call!
        creatives(client, dev_token),
    ])
