
    
    

select
    gsn as unique_field,
    count(*) as n_records

from "datawarehouse".cortex."gsns"
where gsn is not null
group by gsn
having count(*) > 1


