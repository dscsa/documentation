with raw_goodpill_gp_drugs as (
    select
        cast(jsonb_extract_path_text(_airbyte_data, 'drug_generic') as varchar(255)) as drug_generic,
        cast(jsonb_extract_path_text(_airbyte_data, 'drug_brand') as varchar(255)) as drug_brand,
        cast(jsonb_extract_path_text(_airbyte_data, 'drug_gsns') as varchar(255)) as drug_gsns,
        cast(jsonb_extract_path_text(_airbyte_data, 'drug_ordered') as int) as drug_ordered,
        cast(jsonb_extract_path_text(_airbyte_data, 'price30') as int) as price30,
        cast(jsonb_extract_path_text(_airbyte_data, 'price90') as int) as price90,
        cast(jsonb_extract_path_text(_airbyte_data, 'qty_repack') as int) as qty_repack,
        cast(jsonb_extract_path_text(_airbyte_data, 'qty_min') as int) as qty_min,
        cast(jsonb_extract_path_text(_airbyte_data, 'days_min') as int) as days_min,
        cast(jsonb_extract_path_text(_airbyte_data, 'max_inventory') as int) as max_inventory,
        cast(jsonb_extract_path_text(_airbyte_data, 'message_display') as varchar(255)) as message_display,
        cast(jsonb_extract_path_text(_airbyte_data, 'message_verified') as varchar(255)) as message_verified,
        cast(jsonb_extract_path_text(_airbyte_data, 'message_destroyed') as varchar(255)) as message_destroyed,
        cast(jsonb_extract_path_text(_airbyte_data, 'price_goodrx') as decimal(10, 3)) as price_goodrx,
        cast(jsonb_extract_path_text(_airbyte_data, 'price_nadac') as decimal(10, 3)) as price_nadac,
        cast(jsonb_extract_path_text(_airbyte_data, 'price_retail') as decimal(10, 3)) as price_retail,
        cast(jsonb_extract_path_text(_airbyte_data, 'count_ndcs') as int) as count_ndcs,
        coalesce(
            cast(jsonb_extract_path_text(_airbyte_data, 'created_at') as timestamp),
            cast(_airbyte_emitted_at as timestamp without time zone)
        ) as created_at,
        coalesce(
            cast(jsonb_extract_path_text(_airbyte_data, 'updated_at') as timestamp),
            cast(_airbyte_emitted_at as timestamp without time zone)
        ) as updated_at
    from
        "datawarehouse"."raw"._airbyte_raw_goodpill_gp_drugs
)

select
    drug_generic,
    nullif(drug_brand, '') as drug_brand,
    nullif(drug_gsns, '') as drug_gsns,
    drug_ordered,
    price30,
    price90,
    price_retail,
    price_goodrx,
    price_nadac,
    qty_min,
    qty_repack,
    days_min,
    count_ndcs,
    max_inventory,
    nullif(message_display, '') as message_display,
    nullif(message_verified, '') as message_verified,
    nullif(message_destroyed, '') as message_destroyed,
    created_at,
    updated_at
from raw_goodpill_gp_drugs
