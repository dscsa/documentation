with raw_goodpill_gp_clinics as (
    select
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_id') as int) as clinic_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'clinic_name_cp') as varchar(255)) as clinic_name_cp,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'clinic_rx_date_added_first') as timestamp
        ) as clinic_rx_date_added_first,
        cast(
            jsonb_extract_path_text(_airbyte_data, 'clinic_rx_date_added_last') as timestamp
        ) as clinic_rx_date_added_last,
        cast(jsonb_extract_path_text(_airbyte_data, 'verified') as boolean) as verified,
        cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at
    from "datawarehouse"."raw"._airbyte_raw_goodpill_gp_clinics
)

select
    clinic_id,
    nullif(clinic_name_cp, '') as clinic_name_cp,
    clinic_rx_date_added_first,
    clinic_rx_date_added_last,
    verified,
    created_at,
    updated_at
from raw_goodpill_gp_clinics

    where updated_at > (select max(updated_at) from "datawarehouse".dev_analytics."clinics")
