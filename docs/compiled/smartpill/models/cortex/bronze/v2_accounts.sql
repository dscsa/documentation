select
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as bigint) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'v2_id') as varchar(255)) as v2_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'name') as varchar(255)) as name,
    cast(jsonb_extract_path_text(_airbyte_data, 'license') as varchar(255)) as license,
    cast(jsonb_extract_path_text(_airbyte_data, 'street') as varchar(255)) as street,
    cast(jsonb_extract_path_text(_airbyte_data, 'city') as varchar(255)) as city,
    cast(jsonb_extract_path_text(_airbyte_data, 'state') as varchar(255)) as state,
    cast(jsonb_extract_path_text(_airbyte_data, 'zip') as varchar(255)) as zip,
    cast(jsonb_extract_path_text(_airbyte_data, 'phone') as varchar(255)) as phone
from "datawarehouse"."raw"._airbyte_raw_cortex_v2_accounts