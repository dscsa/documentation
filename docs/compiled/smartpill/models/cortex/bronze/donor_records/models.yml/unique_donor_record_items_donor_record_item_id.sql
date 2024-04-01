
    
    

select
    donor_record_item_id as unique_field,
    count(*) as n_records

from "datawarehouse".cortex."donor_record_items"
where donor_record_item_id is not null
group by donor_record_item_id
having count(*) > 1


