with raw_goodpill_dw_clinics as (
    select
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_id') as int) as clinic_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_group_id') as int) as clinic_group_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_name') as varchar(255)) as clinic_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_name_cp') as varchar(255)) as clinic_name_cp,
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_address') as varchar(128)) as clinic_address,
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_street') as varchar(128)) as clinic_street,
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_city') as varchar(128)) as clinic_city,
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_state') as varchar(32)) as clinic_state,
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_zip') as varchar(32)) as clinic_zip,
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_phone') as varchar(32)) as clinic_phone,
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_id_sf') as varchar(20)) as clinic_id_sf,
        cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at
    from "datawarehouse"."raw"._airbyte_raw_goodpill_dw_clinics
)

select
    clinic_id,
    clinic_group_id,
    nullif(clinic_name, '') as clinic_name,
    nullif(clinic_name_cp, '') as clinic_name_cp,
    nullif(clinic_address, '') as clinic_address,
    nullif(clinic_street, '') as clinic_street,
    nullif(clinic_city, '') as clinic_city,
    nullif(clinic_state, '') as clinic_state,
    nullif(clinic_zip, '') as clinic_zip,
    nullif(clinic_phone, '') as clinic_phone,
    nullif(clinic_id_sf, '') as clinic_id_sf,
    created_at,
    updated_at
from raw_goodpill_dw_clinics
