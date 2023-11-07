
    with source_data as (
      select
        *
      from "datawarehouse".cortex."v2_drug_generics"
      
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
          lower('v2_drug_id') as column_name,
          nullif(lower('bigint'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "v2_drug_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "v2_drug_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "v2_drug_id") as distinct_count,
          count(distinct "v2_drug_id") = count(*) as is_unique,
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
          lower('name') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "name") as distinct_count,
          count(distinct "name") = count(*) as is_unique,
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
          lower('strength') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "strength" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "strength") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "strength") as distinct_count,
          count(distinct "strength") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          4 as _column_position
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
  