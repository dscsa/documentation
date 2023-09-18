

    select
        count(1) as row_count
    from "datawarehouse".goodpill."dw_providers"
    having count(1) = 0

