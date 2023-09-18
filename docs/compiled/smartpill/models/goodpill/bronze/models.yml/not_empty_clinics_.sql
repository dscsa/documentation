

    select
        count(1) as row_count
    from "datawarehouse".goodpill."clinics"
    having count(1) = 0

