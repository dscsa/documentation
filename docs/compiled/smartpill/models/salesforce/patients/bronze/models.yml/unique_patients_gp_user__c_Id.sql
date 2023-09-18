
    
    

select
    Id as unique_field,
    count(*) as n_records

from "datawarehouse".salesforce."patients_gp_user__c"
where Id is not null
group by Id
having count(*) > 1


