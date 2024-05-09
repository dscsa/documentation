
    
    

select
    summary_id as unique_field,
    count(*) as n_records

from "datawarehouse".cortex."stock_level_summary"
where summary_id is not null
group by summary_id
having count(*) > 1


