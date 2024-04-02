with q as (

    select *, ROW_NUMBER() OVER(
		partition by jsonb_extract_path_text(_airbyte_data, 'id')
		order by "_airbyte_emitted_at" desc
	) as id_row_number
    from  "datawarehouse"."raw"._airbyte_raw_cortex_v2_ia_shipment_items
    
        where _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "datawarehouse".cortex."v2_ia_shipment_items")
    
)
select
    cast(jsonb_extract_path_text(_airbyte_data, 'id') as bigint) as id,
    cast(jsonb_extract_path_text(_airbyte_data, 'couch_id') as varchar(191)) as couch_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'ndc') as varchar(191)) as ndc,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_brand') as varchar(191)) as drug_brand,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_generic') as varchar(191)) as drug_generic,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_generic_name') as varchar(191)) as drug_generic_name,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_generic_strength') as varchar(191)) as drug_generic_strength,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_form') as varchar(191)) as drug_form,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_price_goodrx') as decimal(8,2)) as drug_price_goodrx,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_price_nadac') as decimal(8,2)) as drug_price_nadac,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_price_retail') as decimal(8,2)) as drug_price_retail,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_price') as decimal(8,2)) as drug_price,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_price_updated_at') as timestamp) as drug_price_updated_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'drug_pkg') as varchar(191)) as drug_pkg,
    cast(jsonb_extract_path_text(_airbyte_data, 'bin') as varchar(191)) as bin,
    cast(jsonb_extract_path_text(_airbyte_data, 'v2_user_id') as varchar(191)) as v2_user_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'v2_shipment_id') as varchar(191)) as v2_shipment_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'v2_donor_id') as varchar(20)) as v2_donor_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'v2_recipient_id') as varchar(191)) as v2_recipient_id,
    cast(jsonb_extract_path_text(_airbyte_data, 'quantity') as decimal(8,2)) as quantity,
    cast(jsonb_extract_path_text(_airbyte_data, 'value') as decimal(8,2)) as value,
    cast(jsonb_extract_path_text(_airbyte_data, 'expires_at') as timestamp) as expires_at,
    cast(jsonb_extract_path_text(_airbyte_data, 'verified_on') as date) as verified_on,
    cast(jsonb_extract_path_text(_airbyte_data, 'refused_on') as date) as refused_on,
    cast(jsonb_extract_path_text(_airbyte_data, 'entered_on') as date) as entered_on,
    cast(jsonb_extract_path_text(_airbyte_data, 'expired_on') as date) as expired_on,
    cast(jsonb_extract_path_text(_airbyte_data, 'disposed_on') as date) as disposed_on,
    cast(jsonb_extract_path_text(_airbyte_data, 'created_on') as date) as created_on,
    cast(jsonb_extract_path_text(_airbyte_data, 'received_on') as date) as received_on,
    cast(jsonb_extract_path_text(_airbyte_data, 'dispensed_on') as date) as dispensed_on,
    cast(jsonb_extract_path_text(_airbyte_data, 'picked_on') as date) as picked_on,
    cast(jsonb_extract_path_text(_airbyte_data, 'repacked_on') as date) as repacked_on,
    cast(jsonb_extract_path_text(_airbyte_data, 'pended_on') as date) as pended_on,
    cast(jsonb_extract_path_text(_airbyte_data, 'next_on') as date) as next_on,
    cast(jsonb_extract_path_text(_airbyte_data, 'removed_on') as date) as removed_on,
    cast(jsonb_extract_path_text(_airbyte_data, 'sorted_bin_1') as varchar(191)) as sorted_bin_1,
    cast(jsonb_extract_path_text(_airbyte_data, 'sorted_bin_2') as varchar(191)) as sorted_bin_2,
    cast(jsonb_extract_path_text(_airbyte_data, 'sorted_bin_3') as varchar(191)) as sorted_bin_3,
    cast(jsonb_extract_path_text(_airbyte_data, 'sorted_bin_4') as varchar(191)) as sorted_bin_4,
    cast(jsonb_extract_path_text(_airbyte_data, 'is_refused') as boolean) as is_refused,
    cast(jsonb_extract_path_text(_airbyte_data, 'is_inventory') as boolean) as is_inventory,
    cast(jsonb_extract_path_text(_airbyte_data, 'is_pended') as boolean) as is_pended,
    cast(jsonb_extract_path_text(_airbyte_data, 'is_dispensed') as boolean) as is_dispensed,
    cast(jsonb_extract_path_text(_airbyte_data, 'is_disposed') as boolean) as is_disposed,
    cast(jsonb_extract_path_text(_airbyte_data, 'is_expired') as boolean) as is_expired,
    cast(jsonb_extract_path_text(_airbyte_data, 'is_magic_bin') as boolean) as is_magic_bin,
    _airbyte_emitted_at
from q
where id_row_number = 1