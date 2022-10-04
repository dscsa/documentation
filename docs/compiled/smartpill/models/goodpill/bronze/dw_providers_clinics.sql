with raw_goodpill_dw_providers_clinics as (
    select
        cast(jsonb_extract_path_text(_airbyte_data, 'provider_clinic_id') as int) as provider_clinic_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_id') as int) as clinic_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'provider_id') as int) as provider_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'started_at') as timestamp) as started_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'stopped_at') as timestamp) as stopped_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at
    from
        "datawarehouse"."raw"._airbyte_raw_goodpill_dw_providers_clinics
)

select
    provider_clinic_id,
    clinic_id,
    provider_id,
    started_at,
    stopped_at,
    created_at,
    updated_at
from raw_goodpill_dw_providers_clinics

    where updated_at > (select max(updated_at) from "datawarehouse".prod_analytics."dw_providers_clinics")
