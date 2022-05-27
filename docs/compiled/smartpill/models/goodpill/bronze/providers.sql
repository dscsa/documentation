

with gp_providers as (
	select
		_airbyte_emitted_at,
		cast(jsonb_extract_path_text(_airbyte_data, 'npi') as varchar) as npi,
		cast(jsonb_extract_path_text(_airbyte_data, 'first_name') as varchar) as first_name,
		cast(jsonb_extract_path_text(_airbyte_data, 'last_name') as varchar) as last_name,
		cast(jsonb_extract_path_text(_airbyte_data, 'first_rx_sent_date') as timestamp) as first_rx_sent_date,
		cast(jsonb_extract_path_text(_airbyte_data, 'last_rx_sent_date') as timestamp) as last_rx_sent_date
	from
		"datawarehouse"."raw"._airbyte_raw_goodpill_gp_providers
)

select
	npi as provider_npi,
	first_name as provider_first_name,
	last_name as provider_last_name,
	first_rx_sent_date as provider_first_rx_sent_date,
	last_rx_sent_date as provider_last_rx_sent_date,
	_airbyte_emitted_at as date_processed
from gp_providers
