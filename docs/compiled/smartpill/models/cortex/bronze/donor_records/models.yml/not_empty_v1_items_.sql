

    select
        count(1) as row_count
    from "datawarehouse".cortex."v1_items"
    having count(1) = 0

