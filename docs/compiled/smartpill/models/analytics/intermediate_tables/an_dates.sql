select
	cast(jsonb_extract_path_text(_airbyte_data, 'date_id') as int) as date_id,
	cast(jsonb_extract_path_text(_airbyte_data, 'db_date') as date) as db_date,
	cast(jsonb_extract_path_text(_airbyte_data, 'year') as int) as year,
	cast(jsonb_extract_path_text(_airbyte_data, 'month') as int) as month,
	cast(jsonb_extract_path_text(_airbyte_data, 'day') as int) as day,
	cast(jsonb_extract_path_text(_airbyte_data, 'week_of_year') as int) as week_of_year,
	cast(jsonb_extract_path_text(_airbyte_data, 'month_name') as varchar) as month_name,
	cast(jsonb_extract_path_text(_airbyte_data, 'day_name') as varchar) as day_name,
	cast(jsonb_extract_path_text(_airbyte_data, 'holiday_flag') as int) as holiday_flag,
	cast(jsonb_extract_path_text(_airbyte_data, 'weekend_flag') as int) as weekend_flag,
	cast(jsonb_extract_path_text(_airbyte_data, 'quarter') as int) as quarter,
	cast(jsonb_extract_path_text(_airbyte_data, 'semester') as int) as semester,
	cast(jsonb_extract_path_text(_airbyte_data, 'date_processed') as timestamp) as date_processed
from "datawarehouse".raw._airbyte_raw_analytics_dates