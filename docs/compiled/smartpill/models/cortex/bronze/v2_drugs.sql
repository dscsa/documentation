select
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as bigint) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'v2_id') as varchar(191)) as v2_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'form') as varchar(191)) as form,
    cast(jsonb_extract_path_text(_airbyte_data, 'brand') as varchar(191)) as brand,
    cast(jsonb_extract_path_text(_airbyte_data, 'labeler') as varchar(191)) as labeler,
    cast(jsonb_extract_path_text(_airbyte_data, 'price_goodrx') as decimal(8,2)) as price_goodrx,
    cast(jsonb_extract_path_text(_airbyte_data, 'price_retail') as decimal(8,2)) as price_retail,
    cast(jsonb_extract_path_text(_airbyte_data, 'price_invalid_at') as timestamp) as price_invalid_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'price_updated_at') as timestamp) as price_updated_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'image') as varchar(191)) as image,
    cast(jsonb_extract_path_text(_airbyte_data, 'ndc9') as varchar(191)) as ndc9,
    cast(jsonb_extract_path_text(_airbyte_data, 'generic') as varchar(191)) as generic,
    cast(jsonb_extract_path_text(_airbyte_data, 'upc') as varchar(191)) as upc,
    cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at

from "datawarehouse"."raw"._airbyte_raw_cortex_v2_drugs

created_at