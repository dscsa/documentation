select
	_airbyte_ab_id,
	_airbyte_emitted_at,
	cast(jsonb_extract_path_text(_airbyte_data, 'provider_id') as int) as provider_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'npi') as varchar(255)) as npi,
	cast(jsonb_extract_path_text(_airbyte_data, 'first_name') as varchar(255)) as first_name,
	cast(jsonb_extract_path_text(_airbyte_data, 'last_name') as varchar(255)) as last_name,
	cast(jsonb_extract_path_text(_airbyte_data, 'phone_number') as varchar(255)) as phone_number,
	cast(jsonb_extract_path_text(_airbyte_data, 'phone_number_2') as varchar(255)) as phone_number_2,
	cast(jsonb_extract_path_text(_airbyte_data, 'address') as varchar(255)) as address,
	cast(jsonb_extract_path_text(_airbyte_data, 'city') as varchar(255)) as city,
	cast(jsonb_extract_path_text(_airbyte_data, 'state') as varchar(255)) as state,
	cast(jsonb_extract_path_text(_airbyte_data, 'zip_code') as varchar(255)) as zip_code,
	cast(jsonb_extract_path_text(_airbyte_data, 'date_processed') as timestamp) as date_processed
from "datawarehouse".raw._airbyte_raw_analytics_providers