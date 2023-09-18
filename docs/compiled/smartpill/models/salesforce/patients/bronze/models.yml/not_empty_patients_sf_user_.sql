

    select
        count(1) as row_count
    from "datawarehouse".salesforce."patients_sf_user"
    having count(1) = 0

