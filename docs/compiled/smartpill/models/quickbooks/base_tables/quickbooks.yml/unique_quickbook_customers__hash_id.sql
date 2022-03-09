
    
    

select
    _hash_id as unique_field,
    count(*) as n_records

from "datawarehouse".prod_analytics."quickbook_customers"
where _hash_id is not null
group by _hash_id
having count(*) > 1


