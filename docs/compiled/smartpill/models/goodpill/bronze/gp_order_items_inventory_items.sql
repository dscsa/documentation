select
    cast(jsonb_extract_path_text(_airbyte_data, 'line_id') as int) as line_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'inventory_item_id') as varchar(255)) as inventory_item_id
from "datawarehouse"."raw"._airbyte_raw_goodpill_gp_order_items_inventory_items