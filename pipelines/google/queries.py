from google.ads.googleads.client import GoogleAdsClient

CREATIVES_QUERY = """
                  SELECT ad_group_ad.ad.id,
                         ad_group_ad.ad.name,
                         ad_group_ad.ad.type,
                         ad_group_ad.ad.final_urls,
                         customer.id,
                  FROM ad_group_ad \
                  """

AD_METRICS_QUERY = """
                   SELECT
                       metrics.average_cost,
                       metrics.average_cpc,
                       metrics.average_cpm,
                       metrics.clicks,
                       metrics.conversions_by_conversion_date,
                       metrics.conversions_value_by_conversion_date,
                       metrics.ctr,
                       metrics.impressions,
                       metrics.video_views,
                       segments.date,
                       ad_group.id,
                       ad_group.name,
                       ad_group.target_roas,
                       ad_group.type,
                       ad_group_ad.ad.call_ad.country_code,
                       ad_group_ad.ad.id,
                       ad_group_ad.ad.name,
                       ad_group_ad.ad.type,
                       campaign.id,
                       campaign.name,
                       campaign.optimization_score,
                       customer.id,
                       customer.descriptive_name
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
