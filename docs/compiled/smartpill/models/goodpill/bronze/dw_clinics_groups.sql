with raw_goodpill_dw_clinic_groups as (
    select
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_group_id') as int) as clinic_group_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_group_name') as varchar(255)) as clinic_group_name,
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_group_id_sf') as varchar(20)) as clinic_group_id_sf,
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_group_domain') as varchar(255)) as clinic_group_domain,
        cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at
    from "datawarehouse"."raw"._airbyte_raw_goodpill_dw_clinic_groups
)

select
    clinic_group_id,
    nullif(clinic_group_name, '') as clinic_group_name,
    nullif(clinic_group_id_sf, '') as clinic_group_id_sf,
    nullif(clinic_group_domain, '') as clinic_group_domain,
    created_at,
    updated_at
from raw_goodpill_dw_clinic_groups

    where updated_at > (select max(updated_at) from "datawarehouse".dev_analytics."dw_clinics_groups")
