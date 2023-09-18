

    select
        count(1) as row_count
    from "datawarehouse".goodpill."dw_clinic_groups"
    having count(1) = 0

