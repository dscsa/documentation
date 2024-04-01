

    select
        count(1) as row_count
    from "datawarehouse".cortex."v2_rs_shipment_items"
    having count(1) = 0

