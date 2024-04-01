with q as (
    select *, ROW_NUMBER() OVER(
		partition by jsonb_extract_path_text(_airbyte_data, 'id')
		order by "_airbyte_emitted_at" desc
	) as id_row_number
    from "datawarehouse"."raw"._airbyte_raw_cortex_v2_shipment_item_stages
    
        where _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "datawarehouse".cortex."v2_shipment_item_stages")
    

)
select
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as bigint) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'v2_shipment_item_id') as bigint) as v2_shipment_item_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'stage') as varchar(191)) as stage,
    cast(jsonb_extract_path_text(_airbyte_data, 'stage_id') as varchar(191)) as stage_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'user_id') as varchar(191)) as user_id,
    _airbyte_emitted_at
from q where id_row_number=1