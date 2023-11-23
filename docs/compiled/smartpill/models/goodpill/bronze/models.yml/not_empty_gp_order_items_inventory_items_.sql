

    select
        count(1) as row_count
    from "datawarehouse".goodpill."gp_order_items_inventory_items"
    having count(1) = 0

