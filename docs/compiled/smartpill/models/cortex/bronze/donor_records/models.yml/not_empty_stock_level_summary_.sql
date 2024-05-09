

    select
        count(1) as row_count
    from "datawarehouse".cortex."stock_level_summary"
    having count(1) = 0

