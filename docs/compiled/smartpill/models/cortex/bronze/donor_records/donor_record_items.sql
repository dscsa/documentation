with q as (
    select *, ROW_NUMBER() OVER(
		partition by jsonb_extract_path_text(_airbyte_data, 'donor_record_item_id')
		order by "_airbyte_emitted_at" desc
	) as id_row_number
    from "datawarehouse"."raw"._airbyte_raw_cortex_donor_record_items
    
    )
select
    cast(jsonb_extract_path_text(_airbyte_data, 'donor_record_item_id') as bigint) as donor_record_item_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'donation_id') as bigint) as donation_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'v1_donation_id') as bigint) as v1_donation_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'v1_id') as bigint) as v1_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'v1_item_id') as bigint) as v1_item_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'sent_qty') as float) as sent_qty,
    cast(jsonb_extract_path_text(_airbyte_data, 'received_qty') as float) as received_qty,
    cast(jsonb_extract_path_text(_airbyte_data, 'accepted_qty') as float) as accepted_qty,
    cast(jsonb_extract_path_text(_airbyte_data, 'dispensed_qty') as float) as dispensed_qty,
    cast(jsonb_extract_path_text(_airbyte_data, 'qty_per_rx') as float) as qty_per_rx,
    cast(jsonb_extract_path_text(_airbyte_data, 'rxs_per_patient') as float) as rxs_per_patient,
    cast(jsonb_extract_path_text(_airbyte_data, 'price') as float) as price,
    cast(jsonb_extract_path_text(_airbyte_data, 'lot_number') as varchar(191)) as lot_number,
    cast(jsonb_extract_path_text(_airbyte_data, 'price_type') as varchar(191)) as price_type,
    cast(jsonb_extract_path_text(_airbyte_data, 'expires_at') as date) as expires_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'price_date') as date) as price_date,

    cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp) as created_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp) as updated_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'archived_at') as timestamp) as archived_at,
    _airbyte_emitted_at
from q where id_row_number=1