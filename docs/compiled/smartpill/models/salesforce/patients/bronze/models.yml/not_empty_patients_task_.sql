

    select
        count(1) as row_count
    from "datawarehouse".salesforce."patients_task"
    having count(1) = 0

