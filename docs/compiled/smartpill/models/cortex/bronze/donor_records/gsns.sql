select
    cast(jsonb_extract_path_text(_airbyte_data, 'gsn') as int) as gsn,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_generic_id') as bigint) as drug_generic_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'form') as varchar(50)) as form,
    cast(jsonb_extract_path_text(_airbyte_data, 'default_pill_weight') as decimal(8,2)) as default_pill_weight,
    cast(jsonb_extract_path_text(_airbyte_data, 'default_bottle_size') as decimal(8,2)) as default_bottle_size,
    cast(jsonb_extract_path_text(_airbyte_data, 'pill_weight_override') as decimal(8,4)) as pill_weight_override,
    cast(jsonb_extract_path_text(_airbyte_data, 'bottle_size_override') as decimal(8,2)) as bottle_size_override,
    cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at
from raw._airbyte_raw_cortex_gsns