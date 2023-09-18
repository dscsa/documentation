

    select
        count(1) as row_count
    from "datawarehouse".goodpill."clinic_coupons"
    having count(1) = 0

