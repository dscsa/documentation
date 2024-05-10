

    select
        count(1) as row_count
    from "datawarehouse".cortex."gsns"
    having count(1) = 0

