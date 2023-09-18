

    select
        count(1) as row_count
    from "datawarehouse".goodpill."drugs"
    having count(1) = 0

