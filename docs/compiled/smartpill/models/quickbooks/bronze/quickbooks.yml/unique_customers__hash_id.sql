
    
    

select
    _hash_id as unique_field,
    count(*) as n_records

from "datawarehouse".quickbooks."customers"
where _hash_id is not null
group by _hash_id
having count(*) > 1


