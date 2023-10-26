select
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as int) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'v2_account_id') as varchar(191)) as v2_account_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_generic') as varchar(191)) as drug_generic,
    cast(jsonb_extract_path_text(_airbyte_data, 'default_location') as varchar(191)) as default_location,
    cast(jsonb_extract_path_text(_airbyte_data, 'display_message') as varchar(191)) as display_message,
    cast(jsonb_extract_path_text(_airbyte_data, 'destroyed_message') as varchar(191)) as destroyed_message,
    cast(jsonb_extract_path_text(_airbyte_data, 'max_inventory') as varchar(191)) as max_inventory,
    cast(jsonb_extract_path_text(_airbyte_data, 'min_days') as varchar(191)) as min_days,
    cast(jsonb_extract_path_text(_airbyte_data, 'min_qty') as varchar(191)) as min_qty,
    cast(jsonb_extract_path_text(_airbyte_data, 'price30') as varchar(191)) as price30,
    cast(jsonb_extract_path_text(_airbyte_data, 'price90') as varchar(191)) as price90,
    cast(jsonb_extract_path_text(_airbyte_data, 'repack_qty') as varchar(191)) as repack_qty,
    cast(jsonb_extract_path_text(_airbyte_data, 'verified_message') as varchar(191)) as verified_message,
    cast(jsonb_extract_path_text(_airbyte_data, 'vial_qty') as varchar(191)) as vial_qty,
    cast(jsonb_extract_path_text(_airbyte_data, 'vial_size') as varchar(191)) as vial_size
from "datawarehouse"."raw"._airbyte_raw_cortex_v2_accounts_ordered