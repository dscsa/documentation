

    select
        count(1) as row_count
    from "datawarehouse".goodpill."gp_pend_group"
    having count(1) = 0

