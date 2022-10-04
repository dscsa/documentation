with final as (
    select
        jsonb_extract_path_text(_airbyte_data, 'Id') as id,
        cast(jsonb_extract_path_text(_airbyte_data, 'MetaData','CreateTime') as timestamp) as created_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'MetaData','LastUpdatedTime') as timestamp) as updated_at,
        _airbyte_emitted_at,
        cast(jsonb_extract_path_text(_airbyte_data, 'Active') as bool) as is_active,
        jsonb_extract_path_text(_airbyte_data, 'AssetAccountRef','value') as asset_account_id,
        jsonb_extract_path_text(_airbyte_data, 'ExpenseAccountRef','value') as expense_account_id,
        jsonb_extract_path_text(_airbyte_data, 'Description') as description,
        jsonb_extract_path_text(_airbyte_data, 'FullyQualifiedName') as fully_qualified_name,
        jsonb_extract_path_text(_airbyte_data, 'IncomeAccountRef','value') as income_account_id,
        jsonb_extract_path_text(_airbyte_data, 'Name') as name,
        jsonb_extract_path_text(_airbyte_data, 'ParentRef','value') as parent_item_id,
        cast(jsonb_extract_path_text(_airbyte_data, 'PurchaseCost') as decimal) as purchase_cost,
        jsonb_extract_path_text(_airbyte_data, 'PurchaseDesc') as purchase_description,
        cast(jsonb_extract_path_text(_airbyte_data, 'SubItem') as int) as sub_item,
        cast(jsonb_extract_path_text(_airbyte_data, 'Taxable') as bool) as taxable,
        cast(jsonb_extract_path_text(_airbyte_data, 'TrackQtyOnHand') as bool) as track_quantity_on_hand,
        jsonb_extract_path_text(_airbyte_data, 'Type') as type,
        cast(jsonb_extract_path_text(_airbyte_data, 'UnitPrice') as decimal) as unit_price
    from
        "datawarehouse"."raw"._airbyte_raw_quickbooks_items
)
select
    *,
    md5("id" || '-' || "_airbyte_emitted_at") as _hash_id
from
    final

where
    _airbyte_emitted_at > (select max(_airbyte_emitted_at) from "datawarehouse".prod_quickbooks."items")
