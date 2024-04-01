

    select
        count(1) as row_count
    from "datawarehouse".cortex."v2_mn_sync_status"
    having count(1) = 0

