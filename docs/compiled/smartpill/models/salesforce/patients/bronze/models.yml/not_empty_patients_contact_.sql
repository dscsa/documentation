

    select
        count(1) as row_count
    from "datawarehouse".salesforce."patients_contact"
    having count(1) = 0

