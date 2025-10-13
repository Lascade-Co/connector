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
                       segments.date,
                       customer.id,
                       customer.descriptive_name,
                       campaign.id,
                       campaign.name,
                       campaign.maximize_conversion_value.target_roas,
                       ad_group.id,
                       ad_group.name,
                       ad_group_ad.ad.id,
                       ad_group_ad.ad.name,
                       ad_group_ad.ad.type,
                       metrics.cost_micros,
                       metrics.impressions,
                       metrics.clicks,
                       metrics.ctr,
                       metrics.average_cpc,
                       metrics.average_cpm,
                       metrics.conversions_by_conversion_date,
                       metrics.conversions_value_by_conversion_date,
                       metrics.conversions,
                       metrics.conversions_value,
                       metrics.cost_per_conversion,
                       metrics.all_conversions,
                       metrics.all_conversions_value,
                       metrics.video_views
                   FROM ad_group_ad
                   WHERE segments.date BETWEEN '{start}' AND '{end}' \
                   """

CAMPAIGN_QUERY = """
                   SELECT
                       campaign.id,
                       campaign.name,
                       campaign_budget.amount_micros,
                       customer.id
                   FROM campaign
                   """

CONVERSION_ACTION_QUERY = """
                   SELECT
                       segments.date,
                       segments.conversion_action_name,
                       segments.conversion_action,
                       customer.id,
                       customer.descriptive_name,
                       campaign.id,
                       campaign.name,
                       ad_group.id,
                       ad_group.name,
                       ad_group_ad.ad.id,
                       ad_group_ad.ad.name,
                       metrics.conversions,
                       metrics.conversions_value,
                       metrics.all_conversions,
                       metrics.all_conversions_value
                   FROM ad_group_ad
                   WHERE segments.date BETWEEN '{start}' AND '{end}'
                   AND segments.conversion_action_name != 'All conversions' \
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
