

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
     + 
    
    p3.generated_number * pow(2, 3)
     + 
    
    p4.generated_number * pow(2, 4)
     + 
    
    p5.generated_number * pow(2, 5)
     + 
    
    p6.generated_number * pow(2, 6)
     + 
    
    p7.generated_number * pow(2, 7)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
    
    

    )

    select *
    from unioned
    where generated_number <= 238
    order by generated_number


    ),
    joined as (
        select
            _hash_id as _airbyte_hashid,
            
            json_extract(line, concat("$[", numbers.generated_number - 1, "][0]")) as _airbyte_nested_data
        from analytics.`quickbook_journal_entries`
        cross join numbers
        -- only generate the number of records in the cross join that corresponds
        -- to the number of items in quickbook_journal_entries.line
        where numbers.generated_number <= json_length(line)
    )
select
    _hash_id as _journal_entry_hash_id,
    id as journal_entry_id,
    json_value(_airbyte_nested_data, 
    '$."Id"') as id,
    cast(json_value(_airbyte_nested_data, 
    '$."Amount"') as 
    float
) as amount,
    json_value(_airbyte_nested_data, 
    '$."JournalEntryLineDetail"."AccountRef"."value"') as account_id,
    json_value(_airbyte_nested_data, 
    '$."JournalEntryLineDetail"."ClassRef"."value"') as class_id,
    json_value(_airbyte_nested_data, 
    '$."JournalEntryLineDetail"."CustomerRef"."value"') as customer_id,
    json_value(_airbyte_nested_data, 
    '$."JournalEntryLineDetail"."PostingType"') as posting_type,
    json_value(_airbyte_nested_data, 
    '$."Description"') as description
from analytics.`quickbook_journal_entries`
left join joined on _hash_id = joined._airbyte_hashid
where
    line is not null
    and 
    _airbyte_emitted_at >= (select max(_airbyte_emitted_at) from analytics.`quickbook_journal_entries`)
