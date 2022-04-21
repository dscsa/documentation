select
    _airbyte_emitted_at,
    _airbyte_ab_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as int) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'provider_name') as varchar(255)) as provider_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'clinic_name') as varchar(255)) as clinic_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'comments') as varchar(255)) as commentary,
    cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at,
    case cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp)
        when '1970-01-01'::timestamp then null
        else cast(jsonb_extract_path_text(_airbyte_data, '_ab_cdc_updated_at') as timestamp)
    end
    as _ab_cdc_updated_at,
    cast(
        jsonb_extract_path_text(_airbyte_data, '_ab_cdc_deleted_at') as timestamp
    ) as _ab_cdc_deleted_at
from
    "datawarehouse".raw._airbyte_raw_goodpill_clinics_meta_provider_names