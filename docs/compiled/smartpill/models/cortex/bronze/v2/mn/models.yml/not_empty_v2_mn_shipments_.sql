

    select
        count(1) as row_count
    from "datawarehouse".cortex."v2_mn_shipments"
    having count(1) = 0

