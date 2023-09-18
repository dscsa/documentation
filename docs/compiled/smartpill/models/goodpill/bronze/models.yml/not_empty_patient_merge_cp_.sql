

    select
        count(1) as row_count
    from "datawarehouse".goodpill."patient_merge_cp"
    having count(1) = 0

