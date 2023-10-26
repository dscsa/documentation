select
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as bigint) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'database') as varchar(191)) as database,
    cast(jsonb_extract_path_text(_airbyte_data, 'document') as text) as document,
    cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at
from "datawarehouse"."raw"._airbyte_raw_cortex_v2_failed_imports