

    select
        count(1) as row_count
    from "datawarehouse".cortex."v2_ia_drug_gsns"
    having count(1) = 0

