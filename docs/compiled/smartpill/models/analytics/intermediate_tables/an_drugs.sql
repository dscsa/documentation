select
	_airbyte_ab_id,
	_airbyte_emitted_at,
	cast(jsonb_extract_path_text(_airbyte_data, 'drug_id') as int) as drug_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'generic_name') as varchar(255)) as generic_name,
	cast(jsonb_extract_path_text(_airbyte_data, 'brand_name') as varchar(255)) as brand_name,
	cast(jsonb_extract_path_text(_airbyte_data, 'gsns') as varchar(255)) as gsns,
	cast(jsonb_extract_path_text(_airbyte_data, 'current_price_per_month') as decimal(10,4)) as current_price_per_month,
	cast(jsonb_extract_path_text(_airbyte_data, 'current_price_30') as decimal(10,4)) as current_price_30,
	cast(jsonb_extract_path_text(_airbyte_data, 'current_price_90') as decimal(10,4)) as current_price_90,
	cast(jsonb_extract_path_text(_airbyte_data, 'current_value_awp') as decimal(10,4)) as current_value_awp,
	cast(jsonb_extract_path_text(_airbyte_data, 'current_value_goodrx') as decimal(10,4)) as current_value_goodrx,
	cast(jsonb_extract_path_text(_airbyte_data, 'current_value_nadac') as decimal(10,4)) as current_value_nadac,
	cast(jsonb_extract_path_text(_airbyte_data, 'current_value_coalesced') as decimal(10,4)) as current_value_coalesced,
	cast(jsonb_extract_path_text(_airbyte_data, 'date_processed') as timestamp) as date_processed
from "datawarehouse".raw._airbyte_raw_analytics_drugs