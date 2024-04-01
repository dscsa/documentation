select
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as bigint) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'v2_drug_id') as bigint) as v2_drug_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'gsn') as varchar(191)) as gsn
from "datawarehouse"."raw"._airbyte_raw_cortex_v2_rs_drug_gsns