
with numbers as (
        

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * pow(2, 0)
     + 
    
    p1.generated_number * pow(2, 1)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
    
    

    )

    select *
    from unioned
    where generated_number <= 4
    order by generated_number


    ),
    joined as (
        select
            _hash_id as _airbyte_hashid,
            
            json_extract(line, concat("$[", numbers.generated_number - 1, "][0]")) as _airbyte_nested_data
        from analytics.`quickbook_invoices`
        cross join numbers
        -- only generate the number of records in the cross join that corresponds
        -- to the number of items in quickbook_invoices.line
        where numbers.generated_number <= json_length(line)
    )
select
    _hash_id as _invoice_hash_id,
    id as invoice_id,
    json_value(_airbyte_nested_data, 
    '$."Id"') as id,
    cast(json_value(_airbyte_nested_data, 
    '$."Amount"') as 
    float
) as amount,
    json_value(_airbyte_nested_data, 
    '$."SalesItemLineDetail"."ItemAccountRef"."value"') as sales_item_account_id,
    json_value(_airbyte_nested_data, 
    '$."SalesItemLineDetail"."ItemRef"."value"') as sales_item_item_id,
    json_value(_airbyte_nested_data, 
    '$."SalesItemLineDetail"."ClassRef"."value"') as sales_item_class_id,
    json_value(_airbyte_nested_data, 
    '$."SalesItemLineDetail"."Qty"') as sales_item_quantity,
    json_value(_airbyte_nested_data, 
    '$."SalesItemLineDetail"."UnitPrice"') as sales_item_unit_price,
    json_value(_airbyte_nested_data, 
    '$."Description"') as description,
    json_value(_airbyte_nested_data, 
    '$."LineNum"') as line_num
from analytics.`quickbook_invoices`
left join joined on _hash_id = joined._airbyte_hashid
where
    line is not null
    and 
    _airbyte_emitted_at >= (select max(_airbyte_emitted_at) from analytics.`quickbook_invoices`)
