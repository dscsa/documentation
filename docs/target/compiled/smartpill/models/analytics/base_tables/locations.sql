WITH locations AS (
    select 
        order_city as city, 
        order_state as state, 
        order_zip as zip_code,
        _airbyte_emitted_at
    from analytics_v2.goodpill_gp_orders
    UNION
    select 
       patient_city as city,
       patient_state as state,
       patient_zip as zip_code,
       _airbyte_emitted_at
    from analytics_v2.goodpill_gp_patients 
)
select distinct 
    cast(city as varchar(255)) as city, 
    cast(state as varchar(255)) as state, 
    cast(zip_code as varchar(255)) as zip_code, 
    NOW() as date_processed
FROM locations 
WHERE zip_code is not null

    and _airbyte_emitted_at > (select MAX(date_processed) from analytics.`locations`)
