
    
    

select
    id as unique_field,
    count(*) as n_records

from "datawarehouse".cortex."v2_ia_drugs"
where id is not null
group by id
having count(*) > 1


