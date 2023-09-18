

    select
        count(1) as row_count
    from "datawarehouse".goodpill."orders"
    having count(1) = 0

