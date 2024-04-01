select
    cast(jsonb_extract_path_text(_airbyte_data, 'db') as varchar(191)) as db,
    cast(jsonb_extract_path_text(_airbyte_data, 'seq') as varchar(500)) as seq,
    cast(jsonb_extract_path_text(_airbyte_data, 'last_sync_started_at') as timestamp) as last_sync_started_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'last_sync_completed_at') as timestamp) as last_sync_completed_at
from "datawarehouse"."raw"._airbyte_raw_cortex_v2_ia_sync_status