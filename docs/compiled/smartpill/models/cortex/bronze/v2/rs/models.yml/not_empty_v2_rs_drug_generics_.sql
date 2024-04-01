

    select
        count(1) as row_count
    from "datawarehouse".cortex."v2_rs_drug_generics"
    having count(1) = 0

