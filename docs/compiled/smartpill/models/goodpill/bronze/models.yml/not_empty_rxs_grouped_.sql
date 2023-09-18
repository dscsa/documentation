

    select
        count(1) as row_count
    from "datawarehouse".goodpill."rxs_grouped"
    having count(1) = 0

