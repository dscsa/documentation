

    select
        count(1) as row_count
    from "datawarehouse".cortex."v2_ia_accounts"
    having count(1) = 0

