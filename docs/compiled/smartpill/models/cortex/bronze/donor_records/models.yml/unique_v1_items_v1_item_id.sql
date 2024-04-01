
    
    

select
    v1_item_id as unique_field,
    count(*) as n_records

from "datawarehouse".cortex."v1_items"
where v1_item_id is not null
group by v1_item_id
having count(*) > 1


