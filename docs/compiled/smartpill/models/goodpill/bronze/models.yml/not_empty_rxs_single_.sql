

    select
        count(1) as row_count
    from "datawarehouse".goodpill."rxs_single"
    having count(1) = 0

