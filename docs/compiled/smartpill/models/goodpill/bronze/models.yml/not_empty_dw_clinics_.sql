

    select
        count(1) as row_count
    from "datawarehouse".goodpill."dw_clinics"
    having count(1) = 0

