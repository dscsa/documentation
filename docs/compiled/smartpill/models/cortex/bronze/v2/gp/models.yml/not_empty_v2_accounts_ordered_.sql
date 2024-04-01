

    select
        count(1) as row_count
    from "datawarehouse".cortex."v2_accounts_ordered"
    having count(1) = 0

