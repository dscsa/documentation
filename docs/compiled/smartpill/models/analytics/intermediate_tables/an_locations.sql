select
	_airbyte_ab_id,
	_airbyte_emitted_at,
	cast(jsonb_extract_path_text(_airbyte_data, 'location_id') as int) as location_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'city') as varchar(255)) as city,
	cast(jsonb_extract_path_text(_airbyte_data, 'state') as varchar(255)) as state,
	cast(jsonb_extract_path_text(_airbyte_data, 'zip_code') as varchar(255)) as zip_code,
	cast(jsonb_extract_path_text(_airbyte_data, 'date_processed') as timestamp) as date_processed
from "datawarehouse".raw._airbyte_raw_analytics_locations