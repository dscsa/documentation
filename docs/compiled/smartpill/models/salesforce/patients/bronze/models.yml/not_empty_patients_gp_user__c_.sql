

    select
        count(1) as row_count
    from "datawarehouse".salesforce."patients_gp_user__c"
    having count(1) = 0

