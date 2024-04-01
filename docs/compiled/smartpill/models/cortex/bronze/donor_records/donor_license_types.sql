select
    cast(jsonb_extract_path_text(_airbyte_data, 'donor_license_type_id') as bigint) as donor_license_type_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'name') as varchar(50)) as name,
    cast(jsonb_extract_path_text(_airbyte_data, 'region') as varchar(20)) as region,
    cast(jsonb_extract_path_text(_airbyte_data, 'donor_type') as varchar(20)) as donor_type
from "datawarehouse"."raw"._airbyte_raw_cortex_donor_license_types