from google.ads.googleads.client import GoogleAdsClient

CREATIVES_QUERY = """
                  SELECT ad_group_ad.ad.id,
                         ad_group_ad.ad.name,
                         ad_group_ad.ad.type,
                         ad_group_ad.ad.final_urls
                  FROM ad_group_ad \
                  """

AD_METRICS_QUERY = """
                   SELECT segments.date,
                          customer.id,
                          campaign.id,
                          campaign.name,
                          ad_group.id,
                          ad_group.name,
                          ad_group_ad.ad.id,
                          ad_group_ad.ad.name,
                          metrics.impressions,
                          metrics.clicks,
                          metrics.ctr,
                          metrics.average_cpc,
                          metrics.cost_per_thousand_impressions,
                          metrics.conversions_value,
                          metrics.conversions_value_per_cost,
                          metrics.reach
                   FROM ad_group_ad
                   WHERE segments.date BETWEEN '{start}' AND '{end}' \
                   """


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
