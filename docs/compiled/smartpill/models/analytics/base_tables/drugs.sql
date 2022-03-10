
with __dbt__cte__gp_drugs as (
select
    _airbyte_emitted_at,
    _airbyte_ab_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_generic') as varchar(255)) as drug_generic,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_brand') as varchar(255)) as drug_brand,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_gsns') as varchar(255)) as drug_gsns,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_ordered') as int) as drug_ordered,
    cast(jsonb_extract_path_text(_airbyte_data, 'price30') as int) as price30,
    cast(jsonb_extract_path_text(_airbyte_data, 'price90') as int) as price90,
    cast(jsonb_extract_path_text(_airbyte_data, 'qty_repack') as int) as qty_repack,
    cast(jsonb_extract_path_text(_airbyte_data, 'qty_min') as int) as qty_min,
    cast(jsonb_extract_path_text(_airbyte_data, 'days_min') as int) as days_min,
    cast(jsonb_extract_path_text(_airbyte_data, 'max_inventory') as int) as max_inventory,
    cast(jsonb_extract_path_text(_airbyte_data, 'message_display') as varchar(255)) as message_display,
    cast(jsonb_extract_path_text(_airbyte_data, 'message_verified') as varchar(255)) as message_verified,
    cast(jsonb_extract_path_text(_airbyte_data, 'message_destroyed') as varchar(255)) as message_destroyed,
    cast(jsonb_extract_path_text(_airbyte_data, 'price_goodrx') as decimal(10,3)) as price_goodrx,
    cast(jsonb_extract_path_text(_airbyte_data, 'price_nadac') as decimal(10,3)) as price_nadac,
    cast(jsonb_extract_path_text(_airbyte_data, 'price_retail') as decimal(10,3)) as price_retail,
    cast(jsonb_extract_path_text(_airbyte_data, 'count_ndcs') as int) as count_ndcs,
    cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
    cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp) as _ab_cdc_updated_at,
    cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_deleted_at') as timestamp) as _ab_cdc_deleted_at
from 
    "datawarehouse".raw._airbyte_raw_goodpill_gp_drugs
)select distinct on (generic_name)
    gpd.drug_generic as generic_name,
    gpd.drug_brand as brand_name,
    gpd.price30, 
    gpd.price90, 
    gpd.price_retail, 
    gpd.price_goodrx, 
    gpd.price_nadac, 
    coalesce(NULLIF(gpd.price_goodrx, 0), NULLIF(gpd.price_nadac, 0), NULLIF(gpd.price_retail, 0)) * 1 as price_coalesced, 
    NOW() as date_processed
from __dbt__cte__gp_drugs gpd

where gpd._airbyte_emitted_at > (select MAX(date_processed) from "datawarehouse".dev_analytics."drugs")
