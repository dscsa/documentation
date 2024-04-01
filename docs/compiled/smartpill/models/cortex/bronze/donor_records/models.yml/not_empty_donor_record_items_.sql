

    select
        count(1) as row_count
    from "datawarehouse".cortex."donor_record_items"
    having count(1) = 0

