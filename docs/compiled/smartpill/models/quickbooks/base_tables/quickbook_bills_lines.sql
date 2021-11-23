
with numbers as (
        

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * pow(2, 0)
     + 
    
    p1.generated_number * pow(2, 1)
     + 
    
    p2.generated_number * pow(2, 2)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
    
    

    )

    select *
    from unioned
    where generated_number <= 6
    order by generated_number


    ),
    joined as (
        select
            _hash_id as _airbyte_hashid,
            
            json_extract(line, concat("$[", numbers.generated_number - 1, "][0]")) as _airbyte_nested_data
        from analytics.`quickbook_bills`
        cross join numbers
        -- only generate the number of records in the cross join that corresponds
        -- to the number of items in quickbook_bills.line
        where numbers.generated_number <= json_length(line)
    )
select
    _hash_id as _bill_hash_id,
    id as bill_id,
    json_value(_airbyte_nested_data, 
    '$."Id"') as id,
    json_value(_airbyte_nested_data, 
    '$."AccountBasedExpenseLineDetail"."AccountRef"."value"') as account_expense_account_id,
    json_value(_airbyte_nested_data, 
    '$."AccountBasedExpenseLineDetail"."ClassRef"."value"') as account_expense_class_id,
    json_value(_airbyte_nested_data, 
    '$."AccountBasedExpenseLineDetail"."CustomerRef"."value"') as account_expense_customer_id,
    json_value(_airbyte_nested_data, 
    '$."AccountBasedExpenseLineDetail"."BillableStatus"') as account_expense_billable_status,
    json_value(_airbyte_nested_data, 
    '$."ItemBasedExpenseLineDetail"."ClassRef"."value"') as item_expense_class_id,
    json_value(_airbyte_nested_data, 
    '$."ItemBasedExpenseLineDetail"."CustomerRef"."value"') as item_expense_customer_id,
    json_value(_airbyte_nested_data, 
    '$."ItemBasedExpenseLineDetail"."BillableStatus"') as item_expense_billable_status,
    json_value(_airbyte_nested_data, 
    '$."ItemBasedExpenseLineDetail"."ItemRef"."value"') as item_expense_item_id,
    json_value(_airbyte_nested_data, 
    '$."Description"') as description,
    cast(json_value(_airbyte_nested_data, 
    '$."Amount"') as 
    float
) as amount
from analytics.`quickbook_bills`
left join joined on _hash_id = joined._airbyte_hashid
where
    line is not null
    and 
    _airbyte_emitted_at >= (select max(_airbyte_emitted_at) from analytics.`quickbook_bills`)
