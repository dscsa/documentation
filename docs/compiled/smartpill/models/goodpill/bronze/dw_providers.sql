with raw_goodpill_dw_providers as (
    select
        _airbyte_emitted_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'provider_id') as int) as provider_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'provider_npi') as varchar) as provider_npi,
        cast(jsonb_extract_path_text(_airbyte_data, 'provider_name') as varchar) as provider_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'provider_phone') as varchar) as provider_phone,
        cast(jsonb_extract_path_text(_airbyte_data, 'provider_id_sf') as varchar) as provider_id_sf,
        cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at
    from
        "datawarehouse"."raw"._airbyte_raw_goodpill_dw_providers
)

select
    provider_id,
    nullif(provider_npi, '') as provider_npi,
    nullif(provider_name, '') as provider_name,
    nullif(provider_phone, '') as provider_phone,
    nullif(provider_id_sf, '') as provider_id_sf,
    created_at,
    updated_at
from raw_goodpill_dw_providers

    where updated_at > (select max(updated_at) from "datawarehouse".dev_analytics."dw_providers")
