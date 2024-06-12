

    select
        count(1) as row_count
    from "datawarehouse".cortex."dynamic_acceptance_summary"
    having count(1) = 0

