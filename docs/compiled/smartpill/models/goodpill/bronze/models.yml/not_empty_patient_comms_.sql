

    select
        count(1) as row_count
    from "datawarehouse".goodpill."patient_comms"
    having count(1) = 0

