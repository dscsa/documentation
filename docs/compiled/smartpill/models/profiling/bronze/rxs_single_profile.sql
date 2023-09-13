
    with source_data as (
      select
        *
      from "datawarehouse".goodpill."rxs_single"
      
    ),

    column_profiles as (
      
        
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
          1 as _column_position
        from source_data

        union all
      
        
        select 
          lower('_airbyte_ab_id') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "_airbyte_ab_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "_airbyte_ab_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "_airbyte_ab_id") as distinct_count,
          count(distinct "_airbyte_ab_id") = count(*) as is_unique,
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
          lower('rx_number') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_number" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_number") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_number") as distinct_count,
          count(distinct "rx_number") = count(*) as is_unique,
          cast(min("rx_number") as varchar) as min,
          cast(max("rx_number") as varchar) as max,
          avg("rx_number") as avg,
          stddev_pop("rx_number") as std_dev_population,
          stddev_samp("rx_number") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          3 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_id_cp') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_id_cp" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_id_cp") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_id_cp") as distinct_count,
          count(distinct "patient_id_cp") = count(*) as is_unique,
          cast(min("patient_id_cp") as varchar) as min,
          cast(max("patient_id_cp") as varchar) as max,
          avg("patient_id_cp") as avg,
          stddev_pop("patient_id_cp") as std_dev_population,
          stddev_samp("patient_id_cp") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          4 as _column_position
        from source_data

        union all
      
        
        select 
          lower('group_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "group_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "group_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "group_id") as distinct_count,
          count(distinct "group_id") = count(*) as is_unique,
          cast(min("group_id") as varchar) as min,
          cast(max("group_id") as varchar) as max,
          avg("group_id") as avg,
          stddev_pop("group_id") as std_dev_population,
          stddev_samp("group_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          5 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_generic') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_generic" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_generic") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_generic") as distinct_count,
          count(distinct "drug_generic") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          6 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_brand') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_brand" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_brand") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_brand") as distinct_count,
          count(distinct "drug_brand") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          7 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_name") as distinct_count,
          count(distinct "drug_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          8 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_message_key') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_message_key" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_message_key") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_message_key") as distinct_count,
          count(distinct "rx_message_key") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          9 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_message_text') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_message_text" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_message_text") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_message_text") as distinct_count,
          count(distinct "rx_message_text") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          10 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_message_date') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_message_date" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_message_date") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_message_date") as distinct_count,
          count(distinct "rx_message_date") = count(*) as is_unique,
          cast(min("rx_message_date") as varchar) as min,
          cast(max("rx_message_date") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          11 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_gsn') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_gsn" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_gsn") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_gsn") as distinct_count,
          count(distinct "rx_gsn") = count(*) as is_unique,
          cast(min("rx_gsn") as varchar) as min,
          cast(max("rx_gsn") as varchar) as max,
          avg("rx_gsn") as avg,
          stddev_pop("rx_gsn") as std_dev_population,
          stddev_samp("rx_gsn") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          12 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_gsns') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_gsns" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_gsns") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_gsns") as distinct_count,
          count(distinct "drug_gsns") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          13 as _column_position
        from source_data

        union all
      
        
        select 
          lower('refills_left') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "refills_left" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "refills_left") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "refills_left") as distinct_count,
          count(distinct "refills_left") = count(*) as is_unique,
          cast(min("refills_left") as varchar) as min,
          cast(max("refills_left") as varchar) as max,
          avg("refills_left") as avg,
          stddev_pop("refills_left") as std_dev_population,
          stddev_samp("refills_left") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          14 as _column_position
        from source_data

        union all
      
        
        select 
          lower('refills_original') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "refills_original" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "refills_original") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "refills_original") as distinct_count,
          count(distinct "refills_original") = count(*) as is_unique,
          cast(min("refills_original") as varchar) as min,
          cast(max("refills_original") as varchar) as max,
          avg("refills_original") as avg,
          stddev_pop("refills_original") as std_dev_population,
          stddev_samp("refills_original") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          15 as _column_position
        from source_data

        union all
      
        
        select 
          lower('qty_left') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "qty_left" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "qty_left") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "qty_left") as distinct_count,
          count(distinct "qty_left") = count(*) as is_unique,
          cast(min("qty_left") as varchar) as min,
          cast(max("qty_left") as varchar) as max,
          avg("qty_left") as avg,
          stddev_pop("qty_left") as std_dev_population,
          stddev_samp("qty_left") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          16 as _column_position
        from source_data

        union all
      
        
        select 
          lower('qty_original') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "qty_original" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "qty_original") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "qty_original") as distinct_count,
          count(distinct "qty_original") = count(*) as is_unique,
          cast(min("qty_original") as varchar) as min,
          cast(max("qty_original") as varchar) as max,
          avg("qty_original") as avg,
          stddev_pop("qty_original") as std_dev_population,
          stddev_samp("qty_original") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          17 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_actual') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_actual") as distinct_count,
          count(distinct "sig_actual") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          18 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_initial') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_initial") as distinct_count,
          count(distinct "sig_initial") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          19 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_clean') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_clean" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_clean") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_clean") as distinct_count,
          count(distinct "sig_clean") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          20 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_qty') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_qty" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_qty") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_qty") as distinct_count,
          count(distinct "sig_qty") = count(*) as is_unique,
          cast(min("sig_qty") as varchar) as min,
          cast(max("sig_qty") as varchar) as max,
          avg("sig_qty") as avg,
          stddev_pop("sig_qty") as std_dev_population,
          stddev_samp("sig_qty") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          21 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_v1_qty') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_v1_qty" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_v1_qty") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_v1_qty") as distinct_count,
          count(distinct "sig_v1_qty") = count(*) as is_unique,
          cast(min("sig_v1_qty") as varchar) as min,
          cast(max("sig_v1_qty") as varchar) as max,
          avg("sig_v1_qty") as avg,
          stddev_pop("sig_v1_qty") as std_dev_population,
          stddev_samp("sig_v1_qty") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          22 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_v1_days') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_v1_days" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_v1_days") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_v1_days") as distinct_count,
          count(distinct "sig_v1_days") = count(*) as is_unique,
          cast(min("sig_v1_days") as varchar) as min,
          cast(max("sig_v1_days") as varchar) as max,
          avg("sig_v1_days") as avg,
          stddev_pop("sig_v1_days") as std_dev_population,
          stddev_samp("sig_v1_days") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          23 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_v1_qty_per_day') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_v1_qty_per_day" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_v1_qty_per_day") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_v1_qty_per_day") as distinct_count,
          count(distinct "sig_v1_qty_per_day") = count(*) as is_unique,
          cast(min("sig_v1_qty_per_day") as varchar) as min,
          cast(max("sig_v1_qty_per_day") as varchar) as max,
          avg("sig_v1_qty_per_day") as avg,
          stddev_pop("sig_v1_qty_per_day") as std_dev_population,
          stddev_samp("sig_v1_qty_per_day") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          24 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_days') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_days" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_days") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_days") as distinct_count,
          count(distinct "sig_days") = count(*) as is_unique,
          cast(min("sig_days") as varchar) as min,
          cast(max("sig_days") as varchar) as max,
          avg("sig_days") as avg,
          stddev_pop("sig_days") as std_dev_population,
          stddev_samp("sig_days") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          25 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_qty_per_day_default') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_qty_per_day_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_qty_per_day_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_qty_per_day_default") as distinct_count,
          count(distinct "sig_qty_per_day_default") = count(*) as is_unique,
          cast(min("sig_qty_per_day_default") as varchar) as min,
          cast(max("sig_qty_per_day_default") as varchar) as max,
          avg("sig_qty_per_day_default") as avg,
          stddev_pop("sig_qty_per_day_default") as std_dev_population,
          stddev_samp("sig_qty_per_day_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          26 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_qty_per_day_actual') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_qty_per_day_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_qty_per_day_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_qty_per_day_actual") as distinct_count,
          count(distinct "sig_qty_per_day_actual") = count(*) as is_unique,
          cast(min("sig_qty_per_day_actual") as varchar) as min,
          cast(max("sig_qty_per_day_actual") as varchar) as max,
          avg("sig_qty_per_day_actual") as avg,
          stddev_pop("sig_qty_per_day_actual") as std_dev_population,
          stddev_samp("sig_qty_per_day_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          27 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_durations') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_durations" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_durations") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_durations") as distinct_count,
          count(distinct "sig_durations") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          28 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_qtys_per_time') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_qtys_per_time" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_qtys_per_time") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_qtys_per_time") as distinct_count,
          count(distinct "sig_qtys_per_time") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          29 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_frequencies') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_frequencies" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_frequencies") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_frequencies") as distinct_count,
          count(distinct "sig_frequencies") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          30 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_frequency_numerators') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_frequency_numerators" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_frequency_numerators") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_frequency_numerators") as distinct_count,
          count(distinct "sig_frequency_numerators") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          31 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_frequency_denominators') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_frequency_denominators" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_frequency_denominators") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_frequency_denominators") as distinct_count,
          count(distinct "sig_frequency_denominators") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          32 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_v2_qty') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_v2_qty" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_v2_qty") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_v2_qty") as distinct_count,
          count(distinct "sig_v2_qty") = count(*) as is_unique,
          cast(min("sig_v2_qty") as varchar) as min,
          cast(max("sig_v2_qty") as varchar) as max,
          avg("sig_v2_qty") as avg,
          stddev_pop("sig_v2_qty") as std_dev_population,
          stddev_samp("sig_v2_qty") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          33 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_v2_days') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_v2_days" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_v2_days") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_v2_days") as distinct_count,
          count(distinct "sig_v2_days") = count(*) as is_unique,
          cast(min("sig_v2_days") as varchar) as min,
          cast(max("sig_v2_days") as varchar) as max,
          avg("sig_v2_days") as avg,
          stddev_pop("sig_v2_days") as std_dev_population,
          stddev_samp("sig_v2_days") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          34 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_v2_qty_per_day') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_v2_qty_per_day" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_v2_qty_per_day") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_v2_qty_per_day") as distinct_count,
          count(distinct "sig_v2_qty_per_day") = count(*) as is_unique,
          cast(min("sig_v2_qty_per_day") as varchar) as min,
          cast(max("sig_v2_qty_per_day") as varchar) as max,
          avg("sig_v2_qty_per_day") as avg,
          stddev_pop("sig_v2_qty_per_day") as std_dev_population,
          stddev_samp("sig_v2_qty_per_day") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          35 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_v2_unit') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_v2_unit" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_v2_unit") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_v2_unit") as distinct_count,
          count(distinct "sig_v2_unit") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          36 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_v2_conf_score') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_v2_conf_score" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_v2_conf_score") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_v2_conf_score") as distinct_count,
          count(distinct "sig_v2_conf_score") = count(*) as is_unique,
          cast(min("sig_v2_conf_score") as varchar) as min,
          cast(max("sig_v2_conf_score") as varchar) as max,
          avg("sig_v2_conf_score") as avg,
          stddev_pop("sig_v2_conf_score") as std_dev_population,
          stddev_samp("sig_v2_conf_score") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          37 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_v2_dosages') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_v2_dosages" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_v2_dosages") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_v2_dosages") as distinct_count,
          count(distinct "sig_v2_dosages") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          38 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_v2_scores') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_v2_scores" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_v2_scores") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_v2_scores") as distinct_count,
          count(distinct "sig_v2_scores") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          39 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_v2_frequencies') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_v2_frequencies" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_v2_frequencies") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_v2_frequencies") as distinct_count,
          count(distinct "sig_v2_frequencies") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          40 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_v2_durations') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_v2_durations" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_v2_durations") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_v2_durations") as distinct_count,
          count(distinct "sig_v2_durations") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          41 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_autofill') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_autofill" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_autofill") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_autofill") as distinct_count,
          count(distinct "rx_autofill") = count(*) as is_unique,
          cast(min("rx_autofill") as varchar) as min,
          cast(max("rx_autofill") as varchar) as max,
          avg("rx_autofill") as avg,
          stddev_pop("rx_autofill") as std_dev_population,
          stddev_samp("rx_autofill") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          42 as _column_position
        from source_data

        union all
      
        
        select 
          lower('refill_date_first') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "refill_date_first" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "refill_date_first") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "refill_date_first") as distinct_count,
          count(distinct "refill_date_first") = count(*) as is_unique,
          cast(min("refill_date_first") as varchar) as min,
          cast(max("refill_date_first") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          43 as _column_position
        from source_data

        union all
      
        
        select 
          lower('refill_date_last') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "refill_date_last" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "refill_date_last") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "refill_date_last") as distinct_count,
          count(distinct "refill_date_last") = count(*) as is_unique,
          cast(min("refill_date_last") as varchar) as min,
          cast(max("refill_date_last") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          44 as _column_position
        from source_data

        union all
      
        
        select 
          lower('refill_date_manual') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "refill_date_manual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "refill_date_manual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "refill_date_manual") as distinct_count,
          count(distinct "refill_date_manual") = count(*) as is_unique,
          cast(min("refill_date_manual") as varchar) as min,
          cast(max("refill_date_manual") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          45 as _column_position
        from source_data

        union all
      
        
        select 
          lower('refill_date_default') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "refill_date_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "refill_date_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "refill_date_default") as distinct_count,
          count(distinct "refill_date_default") = count(*) as is_unique,
          cast(min("refill_date_default") as varchar) as min,
          cast(max("refill_date_default") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          46 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_status') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_status" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_status") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_status") as distinct_count,
          count(distinct "rx_status") = count(*) as is_unique,
          cast(min("rx_status") as varchar) as min,
          cast(max("rx_status") as varchar) as max,
          avg("rx_status") as avg,
          stddev_pop("rx_status") as std_dev_population,
          stddev_samp("rx_status") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          47 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_stage') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_stage" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_stage") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_stage") as distinct_count,
          count(distinct "rx_stage") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          48 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_source') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_source" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_source") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_source") as distinct_count,
          count(distinct "rx_source") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          49 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_date_transferred_out') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_date_transferred_out" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_date_transferred_out") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_date_transferred_out") as distinct_count,
          count(distinct "rx_date_transferred_out") = count(*) as is_unique,
          cast(min("rx_date_transferred_out") as varchar) as min,
          cast(max("rx_date_transferred_out") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          50 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_date_transferred_in') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_date_transferred_in" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_date_transferred_in") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_date_transferred_in") as distinct_count,
          count(distinct "rx_date_transferred_in") = count(*) as is_unique,
          cast(min("rx_date_transferred_in") as varchar) as min,
          cast(max("rx_date_transferred_in") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          51 as _column_position
        from source_data

        union all
      
        
        select 
          lower('provider_npi') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "provider_npi" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "provider_npi") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "provider_npi") as distinct_count,
          count(distinct "provider_npi") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          52 as _column_position
        from source_data

        union all
      
        
        select 
          lower('provider_first_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "provider_first_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "provider_first_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "provider_first_name") as distinct_count,
          count(distinct "provider_first_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          53 as _column_position
        from source_data

        union all
      
        
        select 
          lower('provider_last_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "provider_last_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "provider_last_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "provider_last_name") as distinct_count,
          count(distinct "provider_last_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          54 as _column_position
        from source_data

        union all
      
        
        select 
          lower('provider_clinic') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "provider_clinic" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "provider_clinic") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "provider_clinic") as distinct_count,
          count(distinct "provider_clinic") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          55 as _column_position
        from source_data

        union all
      
        
        select 
          lower('provider_phone') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "provider_phone" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "provider_phone") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "provider_phone") as distinct_count,
          count(distinct "provider_phone") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          56 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_date_changed') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_date_changed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_date_changed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_date_changed") as distinct_count,
          count(distinct "rx_date_changed") = count(*) as is_unique,
          cast(min("rx_date_changed") as varchar) as min,
          cast(max("rx_date_changed") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          57 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_date_expired') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_date_expired" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_date_expired") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_date_expired") as distinct_count,
          count(distinct "rx_date_expired") = count(*) as is_unique,
          cast(min("rx_date_expired") as varchar) as min,
          cast(max("rx_date_expired") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          58 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_date_added') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_date_added" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_date_added") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_date_added") as distinct_count,
          count(distinct "rx_date_added") = count(*) as is_unique,
          cast(min("rx_date_added") as varchar) as min,
          cast(max("rx_date_added") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          59 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_stock_level_initial') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_stock_level_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_stock_level_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_stock_level_initial") as distinct_count,
          count(distinct "rx_stock_level_initial") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          60 as _column_position
        from source_data

        union all
      
        
        select 
          lower('transfer_pharmacy_phone') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "transfer_pharmacy_phone" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "transfer_pharmacy_phone") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "transfer_pharmacy_phone") as distinct_count,
          count(distinct "transfer_pharmacy_phone") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          61 as _column_position
        from source_data

        union all
      
        
        select 
          lower('transfer_pharmacy_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "transfer_pharmacy_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "transfer_pharmacy_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "transfer_pharmacy_name") as distinct_count,
          count(distinct "transfer_pharmacy_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          62 as _column_position
        from source_data

        union all
      
        
        select 
          lower('transfer_pharmacy_fax') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "transfer_pharmacy_fax" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "transfer_pharmacy_fax") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "transfer_pharmacy_fax") as distinct_count,
          count(distinct "transfer_pharmacy_fax") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          63 as _column_position
        from source_data

        union all
      
        
        select 
          lower('transfer_pharmacy_address') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "transfer_pharmacy_address" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "transfer_pharmacy_address") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "transfer_pharmacy_address") as distinct_count,
          count(distinct "transfer_pharmacy_address") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          64 as _column_position
        from source_data

        union all
      
        
        select 
          lower('created_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "created_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "created_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "created_at") as distinct_count,
          count(distinct "created_at") = count(*) as is_unique,
          cast(min("created_at") as varchar) as min,
          cast(max("created_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          65 as _column_position
        from source_data

        union all
      
        
        select 
          lower('updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "updated_at") as distinct_count,
          count(distinct "updated_at") = count(*) as is_unique,
          cast(min("updated_at") as varchar) as min,
          cast(max("updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          66 as _column_position
        from source_data

        union all
      
        
        select 
          lower('status') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "status" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "status") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "status") as distinct_count,
          count(distinct "status") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          67 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_status_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_status_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_status_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_status_updated_at") as distinct_count,
          count(distinct "rx_status_updated_at") = count(*) as is_unique,
          cast(min("rx_status_updated_at") as varchar) as min,
          cast(max("rx_status_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          68 as _column_position
        from source_data

        union all
      
        
        select 
          lower('provider_email') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "provider_email" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "provider_email") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "provider_email") as distinct_count,
          count(distinct "provider_email") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          69 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_id") as distinct_count,
          count(distinct "rx_id") = count(*) as is_unique,
          cast(min("rx_id") as varchar) as min,
          cast(max("rx_id") as varchar) as max,
          avg("rx_id") as avg,
          stddev_pop("rx_id") as std_dev_population,
          stddev_samp("rx_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          70 as _column_position
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
  