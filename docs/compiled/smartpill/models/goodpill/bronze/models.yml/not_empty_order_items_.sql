

    select
        count(1) as row_count
    from "datawarehouse".goodpill."order_items"
    having count(1) = 0

