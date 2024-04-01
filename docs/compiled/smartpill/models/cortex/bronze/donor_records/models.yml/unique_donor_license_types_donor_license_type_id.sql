
    
    

select
    donor_license_type_id as unique_field,
    count(*) as n_records

from "datawarehouse".cortex."donor_license_types"
where donor_license_type_id is not null
group by donor_license_type_id
having count(*) > 1


