

    select
        count(1) as row_count
    from "datawarehouse".cortex."v2_mn_drug_generics"
    having count(1) = 0

