

    select
        count(1) as row_count
    from "datawarehouse".cortex."donations"
    having count(1) = 0

