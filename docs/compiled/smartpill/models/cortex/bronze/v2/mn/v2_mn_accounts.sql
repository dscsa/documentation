select
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as bigint) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'v2_id') as varchar(255)) as v2_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'name') as varchar(255)) as name,
    cast(jsonb_extract_path_text(_airbyte_data, 'license') as varchar(255)) as license,
    cast(jsonb_extract_path_text(_airbyte_data, 'street') as varchar(255)) as street,
    cast(jsonb_extract_path_text(_airbyte_data, 'city') as varchar(255)) as city,
    cast(jsonb_extract_path_text(_airbyte_data, 'state') as varchar(255)) as state,
    cast(jsonb_extract_path_text(_airbyte_data, 'zip') as varchar(255)) as zip,
    cast(jsonb_extract_path_text(_airbyte_data, 'phone') as varchar(255)) as phone,
    cast(jsonb_extract_path_text(_airbyte_data, 'default_max_inventory') as int) as default_max_inventory,
    cast(jsonb_extract_path_text(_airbyte_data, 'default_min_days') as int) as default_min_days,
    cast(jsonb_extract_path_text(_airbyte_data, 'default_min_qty') as int) as default_min_qty,
    cast(jsonb_extract_path_text(_airbyte_data, 'default_price_90') as int) as default_price_90,
    cast(jsonb_extract_path_text(_airbyte_data, 'default_price_30') as int) as default_price_30,
    cast(jsonb_extract_path_text(_airbyte_data, 'default_repack_quantity') as int) as default_repack_quantity
from "datawarehouse"."raw"._airbyte_raw_cortex_v2_mn_accounts