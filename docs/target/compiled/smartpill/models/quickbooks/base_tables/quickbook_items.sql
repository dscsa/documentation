
with final as (
    select
        json_value(_airbyte_data, 
    '$."Id"') as `id`,
    STR_TO_DATE(json_value(_airbyte_data, 
    '$."MetaData"."CreateTime"'), '%Y-%m-%dT%H:%i:%s.%fZ') as `created_at`,
    STR_TO_DATE(json_value(_airbyte_data, 
    '$."MetaData"."LastUpdatedTime"'), '%Y-%m-%dT%H:%i:%s.%fZ') as `updated_at`,
    _airbyte_emitted_at,
        cast(json_value(_airbyte_data, 
    '$."Active"') as 
    signed
 ) as is_active,
        json_value(_airbyte_data, 
    '$."AssetAccountRef"."value"') as asset_account_id,
        json_value(_airbyte_data, 
    '$."ExpenseAccountRef"."value"') as expense_account_id,
        json_value(_airbyte_data, 
    '$."Description"') as description,
        json_value(_airbyte_data, 
    '$."FullyQualifiedName"') as fully_qualified_name,
        json_value(_airbyte_data, 
    '$."IncomeAccountRef"."value"') as income_account_id,
        json_value(_airbyte_data, 
    '$."Name"') as name,
        json_value(_airbyte_data, 
    '$."ParentRef"."value"') as parent_item_id,
        cast(json_value(_airbyte_data, 
    '$."PurchaseCost"') as 
    float
) as purchase_cost,
        json_value(_airbyte_data, 
    '$."PurchaseDesc"') as purchase_description,
        cast(json_value(_airbyte_data, 
    '$."SubItem"') as 
    signed
 ) as sub_item,
        cast(json_value(_airbyte_data, 
    '$."Taxable"') as 
    signed
 ) as taxable,
        cast(json_value(_airbyte_data, 
    '$."TrackQtyOnHand"') as 
    signed
 ) as track_quantity_on_hand,
        json_value(_airbyte_data, 
    '$."Type"') as type,
        cast(json_value(_airbyte_data, 
    '$."UnitPrice"') as 
    signed
 ) as unit_price
    from
        analytics_v2._airbyte_raw_quickbook_items
)
select
    *,
    md5(cast(concat(coalesce(cast(`id` as char), ''), '-', coalesce(cast(`_airbyte_emitted_at` as char), '')) as char)) as _hash_id
from
    final
where
    
    _airbyte_emitted_at > (select max(_airbyte_emitted_at) from analytics.`quickbook_items`)
