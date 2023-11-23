

    select
        count(1) as row_count
    from "datawarehouse".goodpill."gp_stock_live"
    having count(1) = 0

