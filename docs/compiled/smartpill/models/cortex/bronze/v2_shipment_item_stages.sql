select
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as bigint) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'v2_shipment_item_id') as bigint) as v2_shipment_item_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'stage') as varchar(191)) as stage,
    cast(jsonb_extract_path_text(_airbyte_data, 'stage_id') as varchar(191)) as stage_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'user_id') as varchar(191)) as user_id
from "datawarehouse"."raw"._airbyte_raw_cortex_v2_shipment_item_stages