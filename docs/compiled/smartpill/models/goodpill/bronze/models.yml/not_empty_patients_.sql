

    select
        count(1) as row_count
    from "datawarehouse".goodpill."patients"
    having count(1) = 0

