select
    cast(jsonb_extract_path_text(_airbyte_data, 'v1_item_id') as bigint) as v1_item_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'name') as varchar(191)) as name,
    cast(jsonb_extract_path_text(_airbyte_data, 'description') as varchar(191)) as description,
    cast(jsonb_extract_path_text(_airbyte_data, 'upc') as varchar(191)) as upc,
    cast(jsonb_extract_path_text(_airbyte_data, 'upc_origin') as varchar(191)) as upc_origin,
    cast(jsonb_extract_path_text(_airbyte_data, 'qty_per_rx') as varchar(191)) as qty_per_rx,
    cast(jsonb_extract_path_text(_airbyte_data, 'url') as varchar(191)) as url,
    cast(jsonb_extract_path_text(_airbyte_data, 'image') as varchar(191)) as image,
    cast(jsonb_extract_path_text(_airbyte_data, 'color') as varchar(191)) as color,
    cast(jsonb_extract_path_text(_airbyte_data, 'imprint') as varchar(191)) as imprint,
    cast(jsonb_extract_path_text(_airbyte_data, 'shape') as varchar(191)) as shape,
    cast(jsonb_extract_path_text(_airbyte_data, 'size') as int) as size,
    cast(jsonb_extract_path_text(_airbyte_data, 'score') as int) as score,
    cast(jsonb_extract_path_text(_airbyte_data, 'manufacturer') as varchar(191)) as manufacturer,
    cast(jsonb_extract_path_text(_airbyte_data, 'price') as float) as price
from "datawarehouse"."raw"._airbyte_raw_cortex_v1_items