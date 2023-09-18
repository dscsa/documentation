

    select
        count(1) as row_count
    from "datawarehouse".salesforce."donors_user"
    having count(1) = 0

