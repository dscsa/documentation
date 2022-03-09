
with gph as (


        (
            select

                cast('"datawarehouse".prod_analytics."orders_historic"' as 
    varchar
) as _dbt_source_relation,
                

            from "datawarehouse".prod_analytics."orders_historic"
        )

        union all
        

        (
            select

                cast('"datawarehouse".prod_analytics."rxs_historic"' as 
    varchar
) as _dbt_source_relation,
                

            from "datawarehouse".prod_analytics."rxs_historic"
        )

        union all
        

        (
            select

                cast('"datawarehouse".prod_analytics."order_items_historic"' as 
    varchar
) as _dbt_source_relation,
                

            from "datawarehouse".prod_analytics."order_items_historic"
        )

        union all
        

        (
            select

                cast('"datawarehouse".prod_analytics."patients_status_historic"' as 
    varchar
) as _dbt_source_relation,
                

            from "datawarehouse".prod_analytics."patients_status_historic"
        )

        
)
select
	*
from gph
