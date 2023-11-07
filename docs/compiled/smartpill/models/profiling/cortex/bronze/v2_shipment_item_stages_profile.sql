
    with source_data as (
      select
        *
      from "datawarehouse".cortex."v2_shipment_item_stages"
      
    ),

    column_profiles as (
      
        
        select 
          lower('id') as column_name,
          nullif(lower('bigint'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "id") as distinct_count,
          count(distinct "id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          1 as _column_position
        from source_data

        union all
      
        
        select 
          lower('v2_shipment_item_id') as column_name,
          nullif(lower('bigint'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "v2_shipment_item_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "v2_shipment_item_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "v2_shipment_item_id") as distinct_count,
          count(distinct "v2_shipment_item_id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          2 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stage') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stage" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stage") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stage") as distinct_count,
          count(distinct "stage") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          3 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stage_id') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stage_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stage_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stage_id") as distinct_count,
          count(distinct "stage_id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          4 as _column_position
        from source_data

        union all
      
        
        select 
          lower('user_id') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "user_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "user_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "user_id") as distinct_count,
          count(distinct "user_id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          5 as _column_position
        from source_data

        union all
      
        
        select 
          lower('_airbyte_emitted_at') as column_name,
          nullif(lower('timestamp with time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "_airbyte_emitted_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "_airbyte_emitted_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "_airbyte_emitted_at") as distinct_count,
          count(distinct "_airbyte_emitted_at") = count(*) as is_unique,
          cast(min("_airbyte_emitted_at") as varchar) as min,
          cast(max("_airbyte_emitted_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          6 as _column_position
        from source_data

        
      
    )

    select
      column_name,
      data_type,
      
        row_count,
      
        not_null_proportion,
      
        distinct_proportion,
      
        distinct_count,
      
        is_unique,
      
        min,
      
        max,
      
        avg,
      
        std_dev_population,
      
        std_dev_sample,
      
      profiled_at
    from column_profiles
    order by _column_position asc
  