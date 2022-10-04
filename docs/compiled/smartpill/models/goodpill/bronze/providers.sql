with gp_providers as (
    select
        _airbyte_emitted_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'npi') as varchar) as npi,
        cast(jsonb_extract_path_text(_airbyte_data, 'provider_id') as int) as provider_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'first_name') as varchar) as first_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'last_name') as varchar) as last_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'first_rx_sent_date') as timestamp) as first_rx_sent_date,
        cast(jsonb_extract_path_text(_airbyte_data, 'last_rx_sent_date') as timestamp) as last_rx_sent_date,
        cast(jsonb_extract_path_text(_airbyte_data, 'verified') as boolean) as verified,
        cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at
    from
        "datawarehouse"."raw"._airbyte_raw_goodpill_gp_providers
)

select
    npi,
    provider_id,
    nullif(first_name, '') as first_name,
    nullif(last_name, '') as last_name,
    first_rx_sent_date,
    last_rx_sent_date,
    verified,
    created_at,
    updated_at
from gp_providers

    where updated_at > (select max(updated_at) from "datawarehouse".dev_analytics."providers")
