

    select
        count(1) as row_count
    from "datawarehouse".cortex."v2_mn_failed_imports"
    having count(1) = 0

