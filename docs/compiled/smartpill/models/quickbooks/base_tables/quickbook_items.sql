
with final as (
    select
        jsonb_extract_path_text(_airbyte_data, 'Id') as "id",
    STR_TO_DATE(jsonb_extract_path_text(_airbyte_data, 'MetaData','CreateTime'), '%Y-%m-%dT%H:%i:%s.%fZ') as "created_at",
    STR_TO_DATE(jsonb_extract_path_text(_airbyte_data, 'MetaData','LastUpdatedTime'), '%Y-%m-%dT%H:%i:%s.%fZ') as "updated_at",
    _airbyte_emitted_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'Active') as 
    signed
 ) as is_active,
        jsonb_extract_path_text(_airbyte_data, 'AssetAccountRef','value') as asset_account_id,
        jsonb_extract_path_text(_airbyte_data, 'ExpenseAccountRef','value') as expense_account_id,
        jsonb_extract_path_text(_airbyte_data, 'Description') as description,
        jsonb_extract_path_text(_airbyte_data, 'FullyQualifiedName') as fully_qualified_name,
        jsonb_extract_path_text(_airbyte_data, 'IncomeAccountRef','value') as income_account_id,
        jsonb_extract_path_text(_airbyte_data, 'Name') as name,
        jsonb_extract_path_text(_airbyte_data, 'ParentRef','value') as parent_item_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'PurchaseCost') as 
    numeric(28, 6)
) as purchase_cost,
        jsonb_extract_path_text(_airbyte_data, 'PurchaseDesc') as purchase_description,
        cast(jsonb_extract_path_text(_airbyte_data, 'SubItem') as 
    signed
 ) as sub_item,
        cast(jsonb_extract_path_text(_airbyte_data, 'Taxable') as 
    signed
 ) as taxable,
        cast(jsonb_extract_path_text(_airbyte_data, 'TrackQtyOnHand') as 
    signed
 ) as track_quantity_on_hand,
        jsonb_extract_path_text(_airbyte_data, 'Type') as type,
        cast(jsonb_extract_path_text(_airbyte_data, 'UnitPrice') as 
    signed
 ) as unit_price
    from
        "datawarehouse".raw._airbyte_raw_quickbook_items
)
select
    *,
    md5(cast(coalesce(cast("id" as 
    varchar
), '') || '-' || coalesce(cast("_airbyte_emitted_at" as 
    varchar
), '') as 
    varchar
)) as _hash_id
from
    final
where
    
    true
