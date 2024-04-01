select
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as bigint) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'v2_id') as varchar(191)) as v2_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'v2_donor_account_id') as varchar(191)) as v2_donor_account_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'donor_account_name') as varchar(191)) as donor_account_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'v2_recipient_account_id') as varchar(191)) as v2_recipient_account_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'recipient_account_name') as varchar(191)) as recipient_account_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'status') as varchar(191)) as status,
    cast(jsonb_extract_path_text(_airbyte_data, 'tracking') as varchar(191)) as tracking,
    cast(jsonb_extract_path_text(_airbyte_data, 'pickup_at') as timestamp) as pickup_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at
from "datawarehouse"."raw"._airbyte_raw_cortex_v2_ia_shipments