

    select
        count(1) as row_count
    from "datawarehouse".cortex."donor_license_types"
    having count(1) = 0

