select
    gpd.drug_generic, 
    gpd.drug_brand, 
    gsl.price_per_month, 
    gpd.price30, 
    gpd.price90, 
    gpd.price_retail, 
    gpd.price_goodrx, 
    gpd.price_nadac, 
    coalesce(NULLIF(gpd.price_goodrx, 0), NULLIF(gpd.price_nadac, 0), NULLIF(gpd.price_retail, 0)) * 1 as price_coalesced, 
    NOW() as date_processed
from analytics_v2.goodpill_gp_drugs gpd
left join analytics_v2.goodpill_gp_stock_live gsl on gsl.drug_generic = gpd.drug_generic

where gpd._airbyte_emitted_at > (select MAX(date_processed) from analytics.`drugs`)
