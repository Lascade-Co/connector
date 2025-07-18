CREATIVES_QUERY = """
SELECT 
    ad_group_ad.ad.id, ad_group_ad.ad.name, ad_group_ad.ad.type, ad_group_ad.ad.final_urls 
FROM ad_group_ad
"""

AD_METRICS_QUERY = """
SELECT
   segments.date,
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
WHERE segments.date BETWEEN '{start}' AND '{end}'
"""
