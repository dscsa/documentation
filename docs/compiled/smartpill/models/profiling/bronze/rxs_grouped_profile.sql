
    with source_data as (
      select
        *
      from "datawarehouse".goodpill."rxs_grouped"
      
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
          3 as _column_position
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
          4 as _column_position
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
          5 as _column_position
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
          6 as _column_position
        from source_data

        union all
      
        
        select 
          lower('sig_qty_per_day') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "sig_qty_per_day" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "sig_qty_per_day") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "sig_qty_per_day") as distinct_count,
          count(distinct "sig_qty_per_day") = count(*) as is_unique,
          cast(min("sig_qty_per_day") as varchar) as min,
          cast(max("sig_qty_per_day") as varchar) as max,
          avg("sig_qty_per_day") as avg,
          stddev_pop("sig_qty_per_day") as std_dev_population,
          stddev_samp("sig_qty_per_day") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          7 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_message_keys') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_message_keys" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_message_keys") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_message_keys") as distinct_count,
          count(distinct "rx_message_keys") = count(*) as is_unique,
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
          9 as _column_position
        from source_data

        union all
      
        
        select 
          lower('max_gsn') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "max_gsn" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "max_gsn") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "max_gsn") as distinct_count,
          count(distinct "max_gsn") = count(*) as is_unique,
          cast(min("max_gsn") as varchar) as min,
          cast(max("max_gsn") as varchar) as max,
          avg("max_gsn") as avg,
          stddev_pop("max_gsn") as std_dev_population,
          stddev_samp("max_gsn") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          10 as _column_position
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
          11 as _column_position
        from source_data

        union all
      
        
        select 
          lower('refills_total') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "refills_total" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "refills_total") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "refills_total") as distinct_count,
          count(distinct "refills_total") = count(*) as is_unique,
          cast(min("refills_total") as varchar) as min,
          cast(max("refills_total") as varchar) as max,
          avg("refills_total") as avg,
          stddev_pop("refills_total") as std_dev_population,
          stddev_samp("refills_total") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          12 as _column_position
        from source_data

        union all
      
        
        select 
          lower('qty_total') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "qty_total" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "qty_total") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "qty_total") as distinct_count,
          count(distinct "qty_total") = count(*) as is_unique,
          cast(min("qty_total") as varchar) as min,
          cast(max("qty_total") as varchar) as max,
          avg("qty_total") as avg,
          stddev_pop("qty_total") as std_dev_population,
          stddev_samp("qty_total") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          13 as _column_position
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
          14 as _column_position
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
          15 as _column_position
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
          16 as _column_position
        from source_data

        union all
      
        
        select 
          lower('refill_date_next') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "refill_date_next" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "refill_date_next") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "refill_date_next") as distinct_count,
          count(distinct "refill_date_next") = count(*) as is_unique,
          cast(min("refill_date_next") as varchar) as min,
          cast(max("refill_date_next") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          17 as _column_position
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
          18 as _column_position
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
          19 as _column_position
        from source_data

        union all
      
        
        select 
          lower('best_rx_number') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "best_rx_number" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "best_rx_number") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "best_rx_number") as distinct_count,
          count(distinct "best_rx_number") = count(*) as is_unique,
          cast(min("best_rx_number") as varchar) as min,
          cast(max("best_rx_number") as varchar) as max,
          avg("best_rx_number") as avg,
          stddev_pop("best_rx_number") as std_dev_population,
          stddev_samp("best_rx_number") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          20 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_numbers') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_numbers" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_numbers") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_numbers") as distinct_count,
          count(distinct "rx_numbers") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          21 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sources') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sources" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sources") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sources") as distinct_count,
          count(distinct "rx_sources") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          22 as _column_position
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
          23 as _column_position
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
          24 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_date_transferred') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_date_transferred" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_date_transferred") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_date_transferred") as distinct_count,
          count(distinct "rx_date_transferred") = count(*) as is_unique,
          cast(min("rx_date_transferred") as varchar) as min,
          cast(max("rx_date_transferred") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          25 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_added_first_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_added_first_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_added_first_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_added_first_at") as distinct_count,
          count(distinct "rx_added_first_at") = count(*) as is_unique,
          cast(min("rx_added_first_at") as varchar) as min,
          cast(max("rx_added_first_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          26 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_added_last_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_added_last_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_added_last_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_added_last_at") as distinct_count,
          count(distinct "rx_added_last_at") = count(*) as is_unique,
          cast(min("rx_added_last_at") as varchar) as min,
          cast(max("rx_added_last_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          27 as _column_position
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
          28 as _column_position
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
          29 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_inactivated_last_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_inactivated_last_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_inactivated_last_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_inactivated_last_at") as distinct_count,
          count(distinct "rx_inactivated_last_at") = count(*) as is_unique,
          cast(min("rx_inactivated_last_at") as varchar) as min,
          cast(max("rx_inactivated_last_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          30 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_activated_last_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_activated_last_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_activated_last_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_activated_last_at") as distinct_count,
          count(distinct "rx_activated_last_at") = count(*) as is_unique,
          cast(min("rx_activated_last_at") as varchar) as min,
          cast(max("rx_activated_last_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          31 as _column_position
        from source_data

        union all
      
        
        select 
          lower('group_status') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "group_status" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "group_status") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "group_status") as distinct_count,
          count(distinct "group_status") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          32 as _column_position
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
  