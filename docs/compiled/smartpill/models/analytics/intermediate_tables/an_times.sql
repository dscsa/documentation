select
	cast(jsonb_extract_path_text(_airbyte_data, 'time_id') as int) as time_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'hour24') as int) as hour24,
	cast(jsonb_extract_path_text(_airbyte_data, 'hour24_string') as varchar) as hour24_string,
	cast(jsonb_extract_path_text(_airbyte_data, 'hour12') as int) as hour12,
	cast(jsonb_extract_path_text(_airbyte_data, 'hour12_string') as varchar) as hour12_string,
	cast(jsonb_extract_path_text(_airbyte_data, 'minute') as int) as minute,
	cast(jsonb_extract_path_text(_airbyte_data, 'minute_string') as varchar) as minute_string,
	cast(jsonb_extract_path_text(_airbyte_data, 'second') as int) as second,
	cast(jsonb_extract_path_text(_airbyte_data, 'second_string') as varchar) as second_string,
	cast(jsonb_extract_path_text(_airbyte_data, 'hour24_min_string') as varchar) as hour24_min_string,
	cast(jsonb_extract_path_text(_airbyte_data, 'hour24_full_string') as varchar) as hour24_full_string,
	cast(jsonb_extract_path_text(_airbyte_data, 'hour12_min_string') as varchar) as hour12_min_string,
	cast(jsonb_extract_path_text(_airbyte_data, 'hour12_full_string') as varchar) as hour12_full_string,
	cast(jsonb_extract_path_text(_airbyte_data, 'ampm_code') as int) as ampm_code,
	cast(jsonb_extract_path_text(_airbyte_data, 'ampm_string') as varchar) as ampm_string
from "datawarehouse".raw._airbyte_raw_analytics_times