select
	_airbyte_ab_id,
	_airbyte_emitted_at,
	cast(jsonb_extract_path_text(_airbyte_data, 'pharmacy_id') as int) as pharmacy_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'npi') as varchar(255)) as npi,
	cast(jsonb_extract_path_text(_airbyte_data, 'name') as varchar(255)) as name,
	cast(jsonb_extract_path_text(_airbyte_data, 'phone') as varchar(255)) as phone,
	cast(jsonb_extract_path_text(_airbyte_data, 'fax') as varchar(255)) as fax,
	cast(jsonb_extract_path_text(_airbyte_data, 'address') as varchar(255)) as address,
	cast(jsonb_extract_path_text(_airbyte_data, 'date_processed') as timestamp) as date_processed
from "datawarehouse".raw._airbyte_raw_analytics_pharmacies