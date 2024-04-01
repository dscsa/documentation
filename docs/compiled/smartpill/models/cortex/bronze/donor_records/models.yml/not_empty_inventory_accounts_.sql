

    select
        count(1) as row_count
    from "datawarehouse".cortex."inventory_accounts"
    having count(1) = 0

