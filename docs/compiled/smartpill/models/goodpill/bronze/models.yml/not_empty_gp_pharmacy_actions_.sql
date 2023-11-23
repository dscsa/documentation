

    select
        count(1) as row_count
    from "datawarehouse".goodpill."gp_pharmacy_actions"
    having count(1) = 0

