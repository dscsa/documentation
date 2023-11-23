select 
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as bigint) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'user_id') as bigint) as user_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'model_type') as varchar(255)) as model_type,
    cast(jsonb_extract_path_text(_airbyte_data, 'model_id') as bigint) as model_id,
    
    cast(jsonb_extract_path_text(_airbyte_data, 'type') as varchar(255)) as type,
    cast(jsonb_extract_path_text(_airbyte_data, 'reason') as varchar(255)) as reason,
    cast(jsonb_extract_path_text(_airbyte_data, 'status') as varchar(255)) as status,
    cast(jsonb_extract_path_text(_airbyte_data, 'message') as varchar(255)) as message,
    
    cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at,

    cast(jsonb_extract_path_text(_airbyte_data, 'payload') as text) as payload
from
    "datawarehouse"."raw"._airbyte_raw_goodpill_gp_pharmacy_actions