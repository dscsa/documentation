
    with source_data as (
      select
        *
      from "datawarehouse".goodpill."goodpill_abt"
      
    ),

    column_profiles as (
      
        
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
          1 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_invoice_number') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_invoice_number" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_invoice_number") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_invoice_number") as distinct_count,
          count(distinct "order_invoice_number") = count(*) as is_unique,
          cast(min("order_invoice_number") as varchar) as min,
          cast(max("order_invoice_number") as varchar) as max,
          avg("order_invoice_number") as avg,
          stddev_pop("order_invoice_number") as std_dev_population,
          stddev_samp("order_invoice_number") as std_dev_sample,
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
          lower('dw_patient_event_date') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_patient_event_date" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_patient_event_date") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_patient_event_date") as distinct_count,
          count(distinct "dw_patient_event_date") = count(*) as is_unique,
          cast(min("dw_patient_event_date") as varchar) as min,
          cast(max("dw_patient_event_date") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          4 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_patient_status') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_patient_status" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_patient_status") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_patient_status") as distinct_count,
          count(distinct "dw_patient_status") = count(*) as is_unique,
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
          lower('patient_date_active') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_active" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_active") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_active") as distinct_count,
          count(distinct "patient_date_active") = count(*) as is_unique,
          cast(min("patient_date_active") as varchar) as min,
          cast(max("patient_date_active") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          6 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_no_rx') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_no_rx" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_no_rx") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_no_rx") as distinct_count,
          count(distinct "patient_date_no_rx") = count(*) as is_unique,
          cast(min("patient_date_no_rx") as varchar) as min,
          cast(max("patient_date_no_rx") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          7 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_unregistered') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_unregistered" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_unregistered") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_unregistered") as distinct_count,
          count(distinct "patient_date_unregistered") = count(*) as is_unique,
          cast(min("patient_date_unregistered") as varchar) as min,
          cast(max("patient_date_unregistered") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          8 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_deceased') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_deceased" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_deceased") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_deceased") as distinct_count,
          count(distinct "patient_date_deceased") = count(*) as is_unique,
          cast(min("patient_date_deceased") as varchar) as min,
          cast(max("patient_date_deceased") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          9 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_churned_no_fillable_rx') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_churned_no_fillable_rx" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_churned_no_fillable_rx") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_churned_no_fillable_rx") as distinct_count,
          count(distinct "patient_date_churned_no_fillable_rx") = count(*) as is_unique,
          cast(min("patient_date_churned_no_fillable_rx") as varchar) as min,
          cast(max("patient_date_churned_no_fillable_rx") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          10 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_inactive') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_inactive" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_inactive") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_inactive") as distinct_count,
          count(distinct "patient_date_inactive") = count(*) as is_unique,
          cast(min("patient_date_inactive") as varchar) as min,
          cast(max("patient_date_inactive") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          11 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_churned_other') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_churned_other" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_churned_other") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_churned_other") as distinct_count,
          count(distinct "patient_date_churned_other") = count(*) as is_unique,
          cast(min("patient_date_churned_other") as varchar) as min,
          cast(max("patient_date_churned_other") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          12 as _column_position
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
          13 as _column_position
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
          14 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_best_rx_number') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_best_rx_number" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_best_rx_number") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_best_rx_number") as distinct_count,
          count(distinct "rx_best_rx_number") = count(*) as is_unique,
          cast(min("rx_best_rx_number") as varchar) as min,
          cast(max("rx_best_rx_number") as varchar) as max,
          avg("rx_best_rx_number") as avg,
          stddev_pop("rx_best_rx_number") as std_dev_population,
          stddev_samp("rx_best_rx_number") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          15 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_provider_npi') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_provider_npi" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_provider_npi") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_provider_npi") as distinct_count,
          count(distinct "rx_provider_npi") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          16 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_drug_generic') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_drug_generic" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_drug_generic") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_drug_generic") as distinct_count,
          count(distinct "rx_drug_generic") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          17 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_drug_brand') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_drug_brand" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_drug_brand") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_drug_brand") as distinct_count,
          count(distinct "rx_drug_brand") = count(*) as is_unique,
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
          lower('rx_drug_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_drug_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_drug_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_drug_name") as distinct_count,
          count(distinct "rx_drug_name") = count(*) as is_unique,
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
          20 as _column_position
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
          21 as _column_position
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
          22 as _column_position
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
          23 as _column_position
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
          24 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_drug_gsns') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_drug_gsns" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_drug_gsns") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_drug_gsns") as distinct_count,
          count(distinct "rx_drug_gsns") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          25 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_max_gsn') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_max_gsn" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_max_gsn") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_max_gsn") as distinct_count,
          count(distinct "rx_max_gsn") = count(*) as is_unique,
          cast(min("rx_max_gsn") as varchar) as min,
          cast(max("rx_max_gsn") as varchar) as max,
          avg("rx_max_gsn") as avg,
          stddev_pop("rx_max_gsn") as std_dev_population,
          stddev_samp("rx_max_gsn") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          26 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_refills_left') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_refills_left" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_refills_left") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_refills_left") as distinct_count,
          count(distinct "rx_refills_left") = count(*) as is_unique,
          cast(min("rx_refills_left") as varchar) as min,
          cast(max("rx_refills_left") as varchar) as max,
          avg("rx_refills_left") as avg,
          stddev_pop("rx_refills_left") as std_dev_population,
          stddev_samp("rx_refills_left") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          27 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_refills_original') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_refills_original" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_refills_original") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_refills_original") as distinct_count,
          count(distinct "rx_refills_original") = count(*) as is_unique,
          cast(min("rx_refills_original") as varchar) as min,
          cast(max("rx_refills_original") as varchar) as max,
          avg("rx_refills_original") as avg,
          stddev_pop("rx_refills_original") as std_dev_population,
          stddev_samp("rx_refills_original") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          28 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_refills_total') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_refills_total" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_refills_total") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_refills_total") as distinct_count,
          count(distinct "rx_refills_total") = count(*) as is_unique,
          cast(min("rx_refills_total") as varchar) as min,
          cast(max("rx_refills_total") as varchar) as max,
          avg("rx_refills_total") as avg,
          stddev_pop("rx_refills_total") as std_dev_population,
          stddev_samp("rx_refills_total") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          29 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_qty_left') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_qty_left" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_qty_left") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_qty_left") as distinct_count,
          count(distinct "rx_qty_left") = count(*) as is_unique,
          cast(min("rx_qty_left") as varchar) as min,
          cast(max("rx_qty_left") as varchar) as max,
          avg("rx_qty_left") as avg,
          stddev_pop("rx_qty_left") as std_dev_population,
          stddev_samp("rx_qty_left") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          30 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_qty_original') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_qty_original" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_qty_original") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_qty_original") as distinct_count,
          count(distinct "rx_qty_original") = count(*) as is_unique,
          cast(min("rx_qty_original") as varchar) as min,
          cast(max("rx_qty_original") as varchar) as max,
          avg("rx_qty_original") as avg,
          stddev_pop("rx_qty_original") as std_dev_population,
          stddev_samp("rx_qty_original") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          31 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_qty_total') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_qty_total" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_qty_total") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_qty_total") as distinct_count,
          count(distinct "rx_qty_total") = count(*) as is_unique,
          cast(min("rx_qty_total") as varchar) as min,
          cast(max("rx_qty_total") as varchar) as max,
          avg("rx_qty_total") as avg,
          stddev_pop("rx_qty_total") as std_dev_population,
          stddev_samp("rx_qty_total") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          32 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_actual') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_actual") as distinct_count,
          count(distinct "rx_sig_actual") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          33 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_initial') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_initial") as distinct_count,
          count(distinct "rx_sig_initial") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          34 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_clean') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_clean" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_clean") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_clean") as distinct_count,
          count(distinct "rx_sig_clean") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          35 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_qty') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_qty" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_qty") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_qty") as distinct_count,
          count(distinct "rx_sig_qty") = count(*) as is_unique,
          cast(min("rx_sig_qty") as varchar) as min,
          cast(max("rx_sig_qty") as varchar) as max,
          avg("rx_sig_qty") as avg,
          stddev_pop("rx_sig_qty") as std_dev_population,
          stddev_samp("rx_sig_qty") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          36 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_v1_qty') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_v1_qty" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_v1_qty") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_v1_qty") as distinct_count,
          count(distinct "rx_sig_v1_qty") = count(*) as is_unique,
          cast(min("rx_sig_v1_qty") as varchar) as min,
          cast(max("rx_sig_v1_qty") as varchar) as max,
          avg("rx_sig_v1_qty") as avg,
          stddev_pop("rx_sig_v1_qty") as std_dev_population,
          stddev_samp("rx_sig_v1_qty") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          37 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_v1_days') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_v1_days" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_v1_days") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_v1_days") as distinct_count,
          count(distinct "rx_sig_v1_days") = count(*) as is_unique,
          cast(min("rx_sig_v1_days") as varchar) as min,
          cast(max("rx_sig_v1_days") as varchar) as max,
          avg("rx_sig_v1_days") as avg,
          stddev_pop("rx_sig_v1_days") as std_dev_population,
          stddev_samp("rx_sig_v1_days") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          38 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_v1_qty_per_day') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_v1_qty_per_day" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_v1_qty_per_day") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_v1_qty_per_day") as distinct_count,
          count(distinct "rx_sig_v1_qty_per_day") = count(*) as is_unique,
          cast(min("rx_sig_v1_qty_per_day") as varchar) as min,
          cast(max("rx_sig_v1_qty_per_day") as varchar) as max,
          avg("rx_sig_v1_qty_per_day") as avg,
          stddev_pop("rx_sig_v1_qty_per_day") as std_dev_population,
          stddev_samp("rx_sig_v1_qty_per_day") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          39 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_days') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_days" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_days") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_days") as distinct_count,
          count(distinct "rx_sig_days") = count(*) as is_unique,
          cast(min("rx_sig_days") as varchar) as min,
          cast(max("rx_sig_days") as varchar) as max,
          avg("rx_sig_days") as avg,
          stddev_pop("rx_sig_days") as std_dev_population,
          stddev_samp("rx_sig_days") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          40 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_qty_per_day') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_qty_per_day" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_qty_per_day") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_qty_per_day") as distinct_count,
          count(distinct "rx_sig_qty_per_day") = count(*) as is_unique,
          cast(min("rx_sig_qty_per_day") as varchar) as min,
          cast(max("rx_sig_qty_per_day") as varchar) as max,
          avg("rx_sig_qty_per_day") as avg,
          stddev_pop("rx_sig_qty_per_day") as std_dev_population,
          stddev_samp("rx_sig_qty_per_day") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          41 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_qty_per_day_default') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_qty_per_day_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_qty_per_day_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_qty_per_day_default") as distinct_count,
          count(distinct "rx_sig_qty_per_day_default") = count(*) as is_unique,
          cast(min("rx_sig_qty_per_day_default") as varchar) as min,
          cast(max("rx_sig_qty_per_day_default") as varchar) as max,
          avg("rx_sig_qty_per_day_default") as avg,
          stddev_pop("rx_sig_qty_per_day_default") as std_dev_population,
          stddev_samp("rx_sig_qty_per_day_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          42 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_qty_per_day_actual') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_qty_per_day_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_qty_per_day_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_qty_per_day_actual") as distinct_count,
          count(distinct "rx_sig_qty_per_day_actual") = count(*) as is_unique,
          cast(min("rx_sig_qty_per_day_actual") as varchar) as min,
          cast(max("rx_sig_qty_per_day_actual") as varchar) as max,
          avg("rx_sig_qty_per_day_actual") as avg,
          stddev_pop("rx_sig_qty_per_day_actual") as std_dev_population,
          stddev_samp("rx_sig_qty_per_day_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          43 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_durations') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_durations" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_durations") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_durations") as distinct_count,
          count(distinct "rx_sig_durations") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          44 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_qtys_per_time') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_qtys_per_time" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_qtys_per_time") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_qtys_per_time") as distinct_count,
          count(distinct "rx_sig_qtys_per_time") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          45 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_frequencies') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_frequencies" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_frequencies") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_frequencies") as distinct_count,
          count(distinct "rx_sig_frequencies") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          46 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_frequency_numerators') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_frequency_numerators" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_frequency_numerators") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_frequency_numerators") as distinct_count,
          count(distinct "rx_sig_frequency_numerators") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          47 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_frequency_denominators') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_frequency_denominators" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_frequency_denominators") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_frequency_denominators") as distinct_count,
          count(distinct "rx_sig_frequency_denominators") = count(*) as is_unique,
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
          lower('rx_sig_v2_qty') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_v2_qty" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_v2_qty") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_v2_qty") as distinct_count,
          count(distinct "rx_sig_v2_qty") = count(*) as is_unique,
          cast(min("rx_sig_v2_qty") as varchar) as min,
          cast(max("rx_sig_v2_qty") as varchar) as max,
          avg("rx_sig_v2_qty") as avg,
          stddev_pop("rx_sig_v2_qty") as std_dev_population,
          stddev_samp("rx_sig_v2_qty") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          49 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_v2_days') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_v2_days" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_v2_days") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_v2_days") as distinct_count,
          count(distinct "rx_sig_v2_days") = count(*) as is_unique,
          cast(min("rx_sig_v2_days") as varchar) as min,
          cast(max("rx_sig_v2_days") as varchar) as max,
          avg("rx_sig_v2_days") as avg,
          stddev_pop("rx_sig_v2_days") as std_dev_population,
          stddev_samp("rx_sig_v2_days") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          50 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_v2_qty_per_day') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_v2_qty_per_day" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_v2_qty_per_day") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_v2_qty_per_day") as distinct_count,
          count(distinct "rx_sig_v2_qty_per_day") = count(*) as is_unique,
          cast(min("rx_sig_v2_qty_per_day") as varchar) as min,
          cast(max("rx_sig_v2_qty_per_day") as varchar) as max,
          avg("rx_sig_v2_qty_per_day") as avg,
          stddev_pop("rx_sig_v2_qty_per_day") as std_dev_population,
          stddev_samp("rx_sig_v2_qty_per_day") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          51 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_v2_unit') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_v2_unit" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_v2_unit") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_v2_unit") as distinct_count,
          count(distinct "rx_sig_v2_unit") = count(*) as is_unique,
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
          lower('rx_sig_v2_conf_score') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_v2_conf_score" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_v2_conf_score") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_v2_conf_score") as distinct_count,
          count(distinct "rx_sig_v2_conf_score") = count(*) as is_unique,
          cast(min("rx_sig_v2_conf_score") as varchar) as min,
          cast(max("rx_sig_v2_conf_score") as varchar) as max,
          avg("rx_sig_v2_conf_score") as avg,
          stddev_pop("rx_sig_v2_conf_score") as std_dev_population,
          stddev_samp("rx_sig_v2_conf_score") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          53 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_v2_dosages') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_v2_dosages" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_v2_dosages") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_v2_dosages") as distinct_count,
          count(distinct "rx_sig_v2_dosages") = count(*) as is_unique,
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
          lower('rx_sig_v2_scores') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_v2_scores" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_v2_scores") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_v2_scores") as distinct_count,
          count(distinct "rx_sig_v2_scores") = count(*) as is_unique,
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
          lower('rx_sig_v2_frequencies') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_v2_frequencies" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_v2_frequencies") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_v2_frequencies") as distinct_count,
          count(distinct "rx_sig_v2_frequencies") = count(*) as is_unique,
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
          lower('rx_sig_v2_durations') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_v2_durations" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_v2_durations") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_v2_durations") as distinct_count,
          count(distinct "rx_sig_v2_durations") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          57 as _column_position
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
          58 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_refill_date_first') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_refill_date_first" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_refill_date_first") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_refill_date_first") as distinct_count,
          count(distinct "rx_refill_date_first") = count(*) as is_unique,
          cast(min("rx_refill_date_first") as varchar) as min,
          cast(max("rx_refill_date_first") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          59 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_refill_date_last') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_refill_date_last" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_refill_date_last") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_refill_date_last") as distinct_count,
          count(distinct "rx_refill_date_last") = count(*) as is_unique,
          cast(min("rx_refill_date_last") as varchar) as min,
          cast(max("rx_refill_date_last") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          60 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_refill_date_manual') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_refill_date_manual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_refill_date_manual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_refill_date_manual") as distinct_count,
          count(distinct "rx_refill_date_manual") = count(*) as is_unique,
          cast(min("rx_refill_date_manual") as varchar) as min,
          cast(max("rx_refill_date_manual") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          61 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_refill_date_next') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_refill_date_next" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_refill_date_next") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_refill_date_next") as distinct_count,
          count(distinct "rx_refill_date_next") = count(*) as is_unique,
          cast(min("rx_refill_date_next") as varchar) as min,
          cast(max("rx_refill_date_next") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          62 as _column_position
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
          63 as _column_position
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
          64 as _column_position
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
          65 as _column_position
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
          66 as _column_position
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
          67 as _column_position
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
          68 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_clinic_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_clinic_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_clinic_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_clinic_name") as distinct_count,
          count(distinct "rx_clinic_name") = count(*) as is_unique,
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
          70 as _column_position
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
          71 as _column_position
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
          72 as _column_position
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
          73 as _column_position
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
          74 as _column_position
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
          75 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_created_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_created_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_created_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_created_at") as distinct_count,
          count(distinct "rx_created_at") = count(*) as is_unique,
          cast(min("rx_created_at") as varchar) as min,
          cast(max("rx_created_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          76 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_updated_at") as distinct_count,
          count(distinct "rx_updated_at") = count(*) as is_unique,
          cast(min("rx_updated_at") as varchar) as min,
          cast(max("rx_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          77 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_group_created_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_group_created_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_group_created_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_group_created_at") as distinct_count,
          count(distinct "rx_group_created_at") = count(*) as is_unique,
          cast(min("rx_group_created_at") as varchar) as min,
          cast(max("rx_group_created_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          78 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_group_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_group_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_group_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_group_updated_at") as distinct_count,
          count(distinct "rx_group_updated_at") = count(*) as is_unique,
          cast(min("rx_group_updated_at") as varchar) as min,
          cast(max("rx_group_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          79 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_clinic_name_cp') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_clinic_name_cp" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_clinic_name_cp") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_clinic_name_cp") as distinct_count,
          count(distinct "rx_clinic_name_cp") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          80 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rxs_single_status') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rxs_single_status" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rxs_single_status") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rxs_single_status") as distinct_count,
          count(distinct "rxs_single_status") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          81 as _column_position
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
          82 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_provider_email') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_provider_email" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_provider_email") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_provider_email") as distinct_count,
          count(distinct "rx_provider_email") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          83 as _column_position
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
          84 as _column_position
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
          85 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_group_status') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_group_status" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_group_status") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_group_status") as distinct_count,
          count(distinct "rx_group_status") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          86 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_group_drug_generic') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_group_drug_generic" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_group_drug_generic") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_group_drug_generic") as distinct_count,
          count(distinct "rx_group_drug_generic") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          87 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_group_drug_brand') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_group_drug_brand" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_group_drug_brand") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_group_drug_brand") as distinct_count,
          count(distinct "rx_group_drug_brand") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          88 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_group_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_group_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_group_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_group_id") as distinct_count,
          count(distinct "rx_group_id") = count(*) as is_unique,
          cast(min("rx_group_id") as varchar) as min,
          cast(max("rx_group_id") as varchar) as max,
          avg("rx_group_id") as avg,
          stddev_pop("rx_group_id") as std_dev_population,
          stddev_samp("rx_group_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          89 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_group_drug_gsns') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_group_drug_gsns" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_group_drug_gsns") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_group_drug_gsns") as distinct_count,
          count(distinct "rx_group_drug_gsns") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          90 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_group_rx_autofill') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_group_rx_autofill" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_group_rx_autofill") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_group_rx_autofill") as distinct_count,
          count(distinct "rx_group_rx_autofill") = count(*) as is_unique,
          cast(min("rx_group_rx_autofill") as varchar) as min,
          cast(max("rx_group_rx_autofill") as varchar) as max,
          avg("rx_group_rx_autofill") as avg,
          stddev_pop("rx_group_rx_autofill") as std_dev_population,
          stddev_samp("rx_group_rx_autofill") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          91 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_group_refill_date_first') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_group_refill_date_first" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_group_refill_date_first") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_group_refill_date_first") as distinct_count,
          count(distinct "rx_group_refill_date_first") = count(*) as is_unique,
          cast(min("rx_group_refill_date_first") as varchar) as min,
          cast(max("rx_group_refill_date_first") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          92 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_group_refill_date_last') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_group_refill_date_last" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_group_refill_date_last") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_group_refill_date_last") as distinct_count,
          count(distinct "rx_group_refill_date_last") = count(*) as is_unique,
          cast(min("rx_group_refill_date_last") as varchar) as min,
          cast(max("rx_group_refill_date_last") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          93 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_group_refill_date_manual') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_group_refill_date_manual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_group_refill_date_manual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_group_refill_date_manual") as distinct_count,
          count(distinct "rx_group_refill_date_manual") = count(*) as is_unique,
          cast(min("rx_group_refill_date_manual") as varchar) as min,
          cast(max("rx_group_refill_date_manual") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          94 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_group_refill_date_default') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_group_refill_date_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_group_refill_date_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_group_refill_date_default") as distinct_count,
          count(distinct "rx_group_refill_date_default") = count(*) as is_unique,
          cast(min("rx_group_refill_date_default") as varchar) as min,
          cast(max("rx_group_refill_date_default") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          95 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_group_rx_date_changed') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_group_rx_date_changed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_group_rx_date_changed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_group_rx_date_changed") as distinct_count,
          count(distinct "rx_group_rx_date_changed") = count(*) as is_unique,
          cast(min("rx_group_rx_date_changed") as varchar) as min,
          cast(max("rx_group_rx_date_changed") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          96 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_group_rx_date_expired') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_group_rx_date_expired" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_group_rx_date_expired") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_group_rx_date_expired") as distinct_count,
          count(distinct "rx_group_rx_date_expired") = count(*) as is_unique,
          cast(min("rx_group_rx_date_expired") as varchar) as min,
          cast(max("rx_group_rx_date_expired") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          97 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_transfer_pharmacy_phone') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_transfer_pharmacy_phone" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_transfer_pharmacy_phone") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_transfer_pharmacy_phone") as distinct_count,
          count(distinct "rx_transfer_pharmacy_phone") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          98 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_transfer_pharmacy_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_transfer_pharmacy_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_transfer_pharmacy_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_transfer_pharmacy_name") as distinct_count,
          count(distinct "rx_transfer_pharmacy_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          99 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_transfer_pharmacy_fax') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_transfer_pharmacy_fax" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_transfer_pharmacy_fax") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_transfer_pharmacy_fax") as distinct_count,
          count(distinct "rx_transfer_pharmacy_fax") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          100 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_transfer_pharmacy_address') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_transfer_pharmacy_address" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_transfer_pharmacy_address") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_transfer_pharmacy_address") as distinct_count,
          count(distinct "rx_transfer_pharmacy_address") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          101 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_confirmed_by') as column_name,
          nullif(lower('bigint'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_confirmed_by" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_confirmed_by") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_confirmed_by") as distinct_count,
          count(distinct "rx_sig_confirmed_by") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          102 as _column_position
        from source_data

        union all
      
        
        select 
          lower('rx_sig_confirmed_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "rx_sig_confirmed_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "rx_sig_confirmed_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "rx_sig_confirmed_at") as distinct_count,
          count(distinct "rx_sig_confirmed_at") = count(*) as is_unique,
          cast(min("rx_sig_confirmed_at") as varchar) as min,
          cast(max("rx_sig_confirmed_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          103 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_line_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_line_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_line_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_line_id") as distinct_count,
          count(distinct "item_line_id") = count(*) as is_unique,
          cast(min("item_line_id") as varchar) as min,
          cast(max("item_line_id") as varchar) as max,
          avg("item_line_id") as avg,
          stddev_pop("item_line_id") as std_dev_population,
          stddev_samp("item_line_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          104 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_groups') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_groups" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_groups") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_groups") as distinct_count,
          count(distinct "item_groups") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          105 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_rx_dispensed_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_rx_dispensed_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_rx_dispensed_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_rx_dispensed_id") as distinct_count,
          count(distinct "item_rx_dispensed_id") = count(*) as is_unique,
          cast(min("item_rx_dispensed_id") as varchar) as min,
          cast(max("item_rx_dispensed_id") as varchar) as max,
          avg("item_rx_dispensed_id") as avg,
          stddev_pop("item_rx_dispensed_id") as std_dev_population,
          stddev_samp("item_rx_dispensed_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          106 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_stock_level_initial') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_stock_level_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_stock_level_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_stock_level_initial") as distinct_count,
          count(distinct "item_stock_level_initial") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          107 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_rx_message_keys_initial') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_rx_message_keys_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_rx_message_keys_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_rx_message_keys_initial") as distinct_count,
          count(distinct "item_rx_message_keys_initial") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          108 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_patient_autofill_initial') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_patient_autofill_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_patient_autofill_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_patient_autofill_initial") as distinct_count,
          count(distinct "item_patient_autofill_initial") = count(*) as is_unique,
          cast(min("item_patient_autofill_initial") as varchar) as min,
          cast(max("item_patient_autofill_initial") as varchar) as max,
          avg("item_patient_autofill_initial") as avg,
          stddev_pop("item_patient_autofill_initial") as std_dev_population,
          stddev_samp("item_patient_autofill_initial") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          109 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_rx_autofill_initial') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_rx_autofill_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_rx_autofill_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_rx_autofill_initial") as distinct_count,
          count(distinct "item_rx_autofill_initial") = count(*) as is_unique,
          cast(min("item_rx_autofill_initial") as varchar) as min,
          cast(max("item_rx_autofill_initial") as varchar) as max,
          avg("item_rx_autofill_initial") as avg,
          stddev_pop("item_rx_autofill_initial") as std_dev_population,
          stddev_samp("item_rx_autofill_initial") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          110 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_rx_numbers_initial') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_rx_numbers_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_rx_numbers_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_rx_numbers_initial") as distinct_count,
          count(distinct "item_rx_numbers_initial") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          111 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_zscore_initial') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_zscore_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_zscore_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_zscore_initial") as distinct_count,
          count(distinct "item_zscore_initial") = count(*) as is_unique,
          cast(min("item_zscore_initial") as varchar) as min,
          cast(max("item_zscore_initial") as varchar) as max,
          avg("item_zscore_initial") as avg,
          stddev_pop("item_zscore_initial") as std_dev_population,
          stddev_samp("item_zscore_initial") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          112 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_refills_dispensed_default') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_refills_dispensed_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_refills_dispensed_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_refills_dispensed_default") as distinct_count,
          count(distinct "item_refills_dispensed_default") = count(*) as is_unique,
          cast(min("item_refills_dispensed_default") as varchar) as min,
          cast(max("item_refills_dispensed_default") as varchar) as max,
          avg("item_refills_dispensed_default") as avg,
          stddev_pop("item_refills_dispensed_default") as std_dev_population,
          stddev_samp("item_refills_dispensed_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          113 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_refills_dispensed_actual') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_refills_dispensed_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_refills_dispensed_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_refills_dispensed_actual") as distinct_count,
          count(distinct "item_refills_dispensed_actual") = count(*) as is_unique,
          cast(min("item_refills_dispensed_actual") as varchar) as min,
          cast(max("item_refills_dispensed_actual") as varchar) as max,
          avg("item_refills_dispensed_actual") as avg,
          stddev_pop("item_refills_dispensed_actual") as std_dev_population,
          stddev_samp("item_refills_dispensed_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          114 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_days_dispensed_default') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_days_dispensed_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_days_dispensed_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_days_dispensed_default") as distinct_count,
          count(distinct "item_days_dispensed_default") = count(*) as is_unique,
          cast(min("item_days_dispensed_default") as varchar) as min,
          cast(max("item_days_dispensed_default") as varchar) as max,
          avg("item_days_dispensed_default") as avg,
          stddev_pop("item_days_dispensed_default") as std_dev_population,
          stddev_samp("item_days_dispensed_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          115 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_days_dispensed_actual') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_days_dispensed_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_days_dispensed_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_days_dispensed_actual") as distinct_count,
          count(distinct "item_days_dispensed_actual") = count(*) as is_unique,
          cast(min("item_days_dispensed_actual") as varchar) as min,
          cast(max("item_days_dispensed_actual") as varchar) as max,
          avg("item_days_dispensed_actual") as avg,
          stddev_pop("item_days_dispensed_actual") as std_dev_population,
          stddev_samp("item_days_dispensed_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          116 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_qty_dispensed_default') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_qty_dispensed_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_qty_dispensed_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_qty_dispensed_default") as distinct_count,
          count(distinct "item_qty_dispensed_default") = count(*) as is_unique,
          cast(min("item_qty_dispensed_default") as varchar) as min,
          cast(max("item_qty_dispensed_default") as varchar) as max,
          avg("item_qty_dispensed_default") as avg,
          stddev_pop("item_qty_dispensed_default") as std_dev_population,
          stddev_samp("item_qty_dispensed_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          117 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_qty_dispensed_actual') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_qty_dispensed_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_qty_dispensed_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_qty_dispensed_actual") as distinct_count,
          count(distinct "item_qty_dispensed_actual") = count(*) as is_unique,
          cast(min("item_qty_dispensed_actual") as varchar) as min,
          cast(max("item_qty_dispensed_actual") as varchar) as max,
          avg("item_qty_dispensed_actual") as avg,
          stddev_pop("item_qty_dispensed_actual") as std_dev_population,
          stddev_samp("item_qty_dispensed_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          118 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_price_dispensed_default') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_price_dispensed_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_price_dispensed_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_price_dispensed_default") as distinct_count,
          count(distinct "item_price_dispensed_default") = count(*) as is_unique,
          cast(min("item_price_dispensed_default") as varchar) as min,
          cast(max("item_price_dispensed_default") as varchar) as max,
          avg("item_price_dispensed_default") as avg,
          stddev_pop("item_price_dispensed_default") as std_dev_population,
          stddev_samp("item_price_dispensed_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          119 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_price_dispensed_actual') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_price_dispensed_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_price_dispensed_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_price_dispensed_actual") as distinct_count,
          count(distinct "item_price_dispensed_actual") = count(*) as is_unique,
          cast(min("item_price_dispensed_actual") as varchar) as min,
          cast(max("item_price_dispensed_actual") as varchar) as max,
          avg("item_price_dispensed_actual") as avg,
          stddev_pop("item_price_dispensed_actual") as std_dev_population,
          stddev_samp("item_price_dispensed_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          120 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_unit_price_retail_initial') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_unit_price_retail_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_unit_price_retail_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_unit_price_retail_initial") as distinct_count,
          count(distinct "item_unit_price_retail_initial") = count(*) as is_unique,
          cast(min("item_unit_price_retail_initial") as varchar) as min,
          cast(max("item_unit_price_retail_initial") as varchar) as max,
          avg("item_unit_price_retail_initial") as avg,
          stddev_pop("item_unit_price_retail_initial") as std_dev_population,
          stddev_samp("item_unit_price_retail_initial") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          121 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_unit_price_goodrx_initial') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_unit_price_goodrx_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_unit_price_goodrx_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_unit_price_goodrx_initial") as distinct_count,
          count(distinct "item_unit_price_goodrx_initial") = count(*) as is_unique,
          cast(min("item_unit_price_goodrx_initial") as varchar) as min,
          cast(max("item_unit_price_goodrx_initial") as varchar) as max,
          avg("item_unit_price_goodrx_initial") as avg,
          stddev_pop("item_unit_price_goodrx_initial") as std_dev_population,
          stddev_samp("item_unit_price_goodrx_initial") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          122 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_unit_price_nadac_initial') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_unit_price_nadac_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_unit_price_nadac_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_unit_price_nadac_initial") as distinct_count,
          count(distinct "item_unit_price_nadac_initial") = count(*) as is_unique,
          cast(min("item_unit_price_nadac_initial") as varchar) as min,
          cast(max("item_unit_price_nadac_initial") as varchar) as max,
          avg("item_unit_price_nadac_initial") as avg,
          stddev_pop("item_unit_price_nadac_initial") as std_dev_population,
          stddev_samp("item_unit_price_nadac_initial") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          123 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_unit_price_awp_initial') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_unit_price_awp_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_unit_price_awp_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_unit_price_awp_initial") as distinct_count,
          count(distinct "item_unit_price_awp_initial") = count(*) as is_unique,
          cast(min("item_unit_price_awp_initial") as varchar) as min,
          cast(max("item_unit_price_awp_initial") as varchar) as max,
          avg("item_unit_price_awp_initial") as avg,
          stddev_pop("item_unit_price_awp_initial") as std_dev_population,
          stddev_samp("item_unit_price_awp_initial") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          124 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_qty_pended_total') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_qty_pended_total" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_qty_pended_total") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_qty_pended_total") as distinct_count,
          count(distinct "item_qty_pended_total") = count(*) as is_unique,
          cast(min("item_qty_pended_total") as varchar) as min,
          cast(max("item_qty_pended_total") as varchar) as max,
          avg("item_qty_pended_total") as avg,
          stddev_pop("item_qty_pended_total") as std_dev_population,
          stddev_samp("item_qty_pended_total") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          125 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_qty_pended_repacks') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_qty_pended_repacks" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_qty_pended_repacks") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_qty_pended_repacks") as distinct_count,
          count(distinct "item_qty_pended_repacks") = count(*) as is_unique,
          cast(min("item_qty_pended_repacks") as varchar) as min,
          cast(max("item_qty_pended_repacks") as varchar) as max,
          avg("item_qty_pended_repacks") as avg,
          stddev_pop("item_qty_pended_repacks") as std_dev_population,
          stddev_samp("item_qty_pended_repacks") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          126 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_count_pended_total') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_count_pended_total" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_count_pended_total") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_count_pended_total") as distinct_count,
          count(distinct "item_count_pended_total") = count(*) as is_unique,
          cast(min("item_count_pended_total") as varchar) as min,
          cast(max("item_count_pended_total") as varchar) as max,
          avg("item_count_pended_total") as avg,
          stddev_pop("item_count_pended_total") as std_dev_population,
          stddev_samp("item_count_pended_total") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          127 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_count_pended_repacks') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_count_pended_repacks" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_count_pended_repacks") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_count_pended_repacks") as distinct_count,
          count(distinct "item_count_pended_repacks") = count(*) as is_unique,
          cast(min("item_count_pended_repacks") as varchar) as min,
          cast(max("item_count_pended_repacks") as varchar) as max,
          avg("item_count_pended_repacks") as avg,
          stddev_pop("item_count_pended_repacks") as std_dev_population,
          stddev_samp("item_count_pended_repacks") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          128 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_message_keys') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_message_keys" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_message_keys") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_message_keys") as distinct_count,
          count(distinct "item_message_keys") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          129 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_message_text') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_message_text" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_message_text") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_message_text") as distinct_count,
          count(distinct "item_message_text") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          130 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_type') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_type" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_type") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_type") as distinct_count,
          count(distinct "item_type") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          131 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_added_by') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_added_by" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_added_by") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_added_by") as distinct_count,
          count(distinct "item_added_by") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          132 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_date_added') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_date_added" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_date_added") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_date_added") as distinct_count,
          count(distinct "item_date_added") = count(*) as is_unique,
          cast(min("item_date_added") as varchar) as min,
          cast(max("item_date_added") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          133 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_date_changed') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_date_changed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_date_changed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_date_changed") as distinct_count,
          count(distinct "item_date_changed") = count(*) as is_unique,
          cast(min("item_date_changed") as varchar) as min,
          cast(max("item_date_changed") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          134 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_date_updated') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_date_updated" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_date_updated") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_date_updated") as distinct_count,
          count(distinct "item_date_updated") = count(*) as is_unique,
          cast(min("item_date_updated") as varchar) as min,
          cast(max("item_date_updated") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          135 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_days_and_message_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_days_and_message_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_days_and_message_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_days_and_message_updated_at") as distinct_count,
          count(distinct "item_days_and_message_updated_at") = count(*) as is_unique,
          cast(min("item_days_and_message_updated_at") as varchar) as min,
          cast(max("item_days_and_message_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          136 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_days_and_message_initial_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_days_and_message_initial_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_days_and_message_initial_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_days_and_message_initial_at") as distinct_count,
          count(distinct "item_days_and_message_initial_at") = count(*) as is_unique,
          cast(min("item_days_and_message_initial_at") as varchar) as min,
          cast(max("item_days_and_message_initial_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          137 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_days_pended') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_days_pended" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_days_pended") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_days_pended") as distinct_count,
          count(distinct "item_days_pended") = count(*) as is_unique,
          cast(min("item_days_pended") as varchar) as min,
          cast(max("item_days_pended") as varchar) as max,
          avg("item_days_pended") as avg,
          stddev_pop("item_days_pended") as std_dev_population,
          stddev_samp("item_days_pended") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          138 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_qty_per_day_pended') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_qty_per_day_pended" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_qty_per_day_pended") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_qty_per_day_pended") as distinct_count,
          count(distinct "item_qty_per_day_pended") = count(*) as is_unique,
          cast(min("item_qty_per_day_pended") as varchar) as min,
          cast(max("item_qty_per_day_pended") as varchar) as max,
          avg("item_qty_per_day_pended") as avg,
          stddev_pop("item_qty_per_day_pended") as std_dev_population,
          stddev_samp("item_qty_per_day_pended") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          139 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_refill_date_last') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_refill_date_last" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_refill_date_last") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_refill_date_last") as distinct_count,
          count(distinct "item_refill_date_last") = count(*) as is_unique,
          cast(min("item_refill_date_last") as varchar) as min,
          cast(max("item_refill_date_last") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          140 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_refill_date_manual') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_refill_date_manual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_refill_date_manual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_refill_date_manual") as distinct_count,
          count(distinct "item_refill_date_manual") = count(*) as is_unique,
          cast(min("item_refill_date_manual") as varchar) as min,
          cast(max("item_refill_date_manual") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          141 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_refill_date_default') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_refill_date_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_refill_date_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_refill_date_default") as distinct_count,
          count(distinct "item_refill_date_default") = count(*) as is_unique,
          cast(min("item_refill_date_default") as varchar) as min,
          cast(max("item_refill_date_default") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          142 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_add_user_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_add_user_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_add_user_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_add_user_id") as distinct_count,
          count(distinct "item_add_user_id") = count(*) as is_unique,
          cast(min("item_add_user_id") as varchar) as min,
          cast(max("item_add_user_id") as varchar) as max,
          avg("item_add_user_id") as avg,
          stddev_pop("item_add_user_id") as std_dev_population,
          stddev_samp("item_add_user_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          143 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_chg_user_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_chg_user_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_chg_user_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_chg_user_id") as distinct_count,
          count(distinct "item_chg_user_id") = count(*) as is_unique,
          cast(min("item_chg_user_id") as varchar) as min,
          cast(max("item_chg_user_id") as varchar) as max,
          avg("item_chg_user_id") as avg,
          stddev_pop("item_chg_user_id") as std_dev_population,
          stddev_samp("item_chg_user_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          144 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_count_lines') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_count_lines" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_count_lines") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_count_lines") as distinct_count,
          count(distinct "item_count_lines") = count(*) as is_unique,
          cast(min("item_count_lines") as varchar) as min,
          cast(max("item_count_lines") as varchar) as max,
          avg("item_count_lines") as avg,
          stddev_pop("item_count_lines") as std_dev_population,
          stddev_samp("item_count_lines") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          145 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_repacked_by') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_repacked_by" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_repacked_by") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_repacked_by") as distinct_count,
          count(distinct "item_repacked_by") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          146 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_unpended_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_unpended_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_unpended_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_unpended_at") as distinct_count,
          count(distinct "item_unpended_at") = count(*) as is_unique,
          cast(min("item_unpended_at") as varchar) as min,
          cast(max("item_unpended_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          147 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_pend_initial_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_pend_initial_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_pend_initial_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_pend_initial_at") as distinct_count,
          count(distinct "item_pend_initial_at") = count(*) as is_unique,
          cast(min("item_pend_initial_at") as varchar) as min,
          cast(max("item_pend_initial_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          148 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_pend_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_pend_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_pend_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_pend_updated_at") as distinct_count,
          count(distinct "item_pend_updated_at") = count(*) as is_unique,
          cast(min("item_pend_updated_at") as varchar) as min,
          cast(max("item_pend_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          149 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_ndc_pended') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_ndc_pended" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_ndc_pended") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_ndc_pended") as distinct_count,
          count(distinct "item_ndc_pended") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          150 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_drug_generic_pended') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_drug_generic_pended" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_drug_generic_pended") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_drug_generic_pended") as distinct_count,
          count(distinct "item_drug_generic_pended") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          151 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_filled_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_filled_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_filled_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_filled_at") as distinct_count,
          count(distinct "item_filled_at") = count(*) as is_unique,
          cast(min("item_filled_at") as varchar) as min,
          cast(max("item_filled_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          152 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_pend_failed_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_pend_failed_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_pend_failed_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_pend_failed_at") as distinct_count,
          count(distinct "item_pend_failed_at") = count(*) as is_unique,
          cast(min("item_pend_failed_at") as varchar) as min,
          cast(max("item_pend_failed_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          153 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_filled_by') as column_name,
          nullif(lower('bigint'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_filled_by" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_filled_by") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_filled_by") as distinct_count,
          count(distinct "item_filled_by") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          154 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_pend_retried_by') as column_name,
          nullif(lower('bigint'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_pend_retried_by" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_pend_retried_by") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_pend_retried_by") as distinct_count,
          count(distinct "item_pend_retried_by") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          155 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_status') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_status" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_status") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_status") as distinct_count,
          count(distinct "item_status") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          156 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_pend_retried_days') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_pend_retried_days" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_pend_retried_days") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_pend_retried_days") as distinct_count,
          count(distinct "item_pend_retried_days") = count(*) as is_unique,
          cast(min("item_pend_retried_days") as varchar) as min,
          cast(max("item_pend_retried_days") as varchar) as max,
          avg("item_pend_retried_days") as avg,
          stddev_pop("item_pend_retried_days") as std_dev_population,
          stddev_samp("item_pend_retried_days") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          157 as _column_position
        from source_data

        union all
      
        
        select 
          lower('item_pend_retried_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "item_pend_retried_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "item_pend_retried_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "item_pend_retried_at") as distinct_count,
          count(distinct "item_pend_retried_at") = count(*) as is_unique,
          cast(min("item_pend_retried_at") as varchar) as min,
          cast(max("item_pend_retried_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          158 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_added') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_added" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_added") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_added") as distinct_count,
          count(distinct "order_date_added") = count(*) as is_unique,
          cast(min("order_date_added") as varchar) as min,
          cast(max("order_date_added") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          159 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_dispensed') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_dispensed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_dispensed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_dispensed") as distinct_count,
          count(distinct "order_date_dispensed") = count(*) as is_unique,
          cast(min("order_date_dispensed") as varchar) as min,
          cast(max("order_date_dispensed") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          160 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_shipped') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_shipped" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_shipped") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_shipped") as distinct_count,
          count(distinct "order_date_shipped") = count(*) as is_unique,
          cast(min("order_date_shipped") as varchar) as min,
          cast(max("order_date_shipped") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          161 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_returned') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_returned" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_returned") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_returned") as distinct_count,
          count(distinct "order_date_returned") = count(*) as is_unique,
          cast(min("order_date_returned") as varchar) as min,
          cast(max("order_date_returned") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          162 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_count_items') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_count_items" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_count_items") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_count_items") as distinct_count,
          count(distinct "order_count_items") = count(*) as is_unique,
          cast(min("order_count_items") as varchar) as min,
          cast(max("order_count_items") as varchar) as max,
          avg("order_count_items") as avg,
          stddev_pop("order_count_items") as std_dev_population,
          stddev_samp("order_count_items") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          163 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_count_filled') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_count_filled" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_count_filled") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_count_filled") as distinct_count,
          count(distinct "order_count_filled") = count(*) as is_unique,
          cast(min("order_count_filled") as varchar) as min,
          cast(max("order_count_filled") as varchar) as max,
          avg("order_count_filled") as avg,
          stddev_pop("order_count_filled") as std_dev_population,
          stddev_samp("order_count_filled") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          164 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_count_nofill') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_count_nofill" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_count_nofill") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_count_nofill") as distinct_count,
          count(distinct "order_count_nofill") = count(*) as is_unique,
          cast(min("order_count_nofill") as varchar) as min,
          cast(max("order_count_nofill") as varchar) as max,
          avg("order_count_nofill") as avg,
          stddev_pop("order_count_nofill") as std_dev_population,
          stddev_samp("order_count_nofill") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          165 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_priority') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_priority" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_priority") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_priority") as distinct_count,
          count(distinct "order_priority") = count(*) as is_unique,
          cast(min("order_priority") as varchar) as min,
          cast(max("order_priority") as varchar) as max,
          avg("order_priority") as avg,
          stddev_pop("order_priority") as std_dev_population,
          stddev_samp("order_priority") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          166 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_source') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_source" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_source") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_source") as distinct_count,
          count(distinct "order_source") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          167 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_stage_cp') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_stage_cp" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_stage_cp") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_stage_cp") as distinct_count,
          count(distinct "order_stage_cp") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          168 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_stage_wc') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_stage_wc" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_stage_wc") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_stage_wc") as distinct_count,
          count(distinct "order_stage_wc") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          169 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_status') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_status" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_status") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_status") as distinct_count,
          count(distinct "order_status") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          170 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_address1') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_address1" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_address1") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_address1") as distinct_count,
          count(distinct "order_address1") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          171 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_address2') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_address2" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_address2") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_address2") as distinct_count,
          count(distinct "order_address2") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          172 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_invoice_doc_id') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_invoice_doc_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_invoice_doc_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_invoice_doc_id") as distinct_count,
          count(distinct "order_invoice_doc_id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          173 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_tracking_number') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_tracking_number" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_tracking_number") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_tracking_number") as distinct_count,
          count(distinct "order_tracking_number") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          174 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_payment_total_default') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_payment_total_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_payment_total_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_payment_total_default") as distinct_count,
          count(distinct "order_payment_total_default") = count(*) as is_unique,
          cast(min("order_payment_total_default") as varchar) as min,
          cast(max("order_payment_total_default") as varchar) as max,
          avg("order_payment_total_default") as avg,
          stddev_pop("order_payment_total_default") as std_dev_population,
          stddev_samp("order_payment_total_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          175 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_payment_total_actual') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_payment_total_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_payment_total_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_payment_total_actual") as distinct_count,
          count(distinct "order_payment_total_actual") = count(*) as is_unique,
          cast(min("order_payment_total_actual") as varchar) as min,
          cast(max("order_payment_total_actual") as varchar) as max,
          avg("order_payment_total_actual") as avg,
          stddev_pop("order_payment_total_actual") as std_dev_population,
          stddev_samp("order_payment_total_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          176 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_payment_fee_default') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_payment_fee_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_payment_fee_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_payment_fee_default") as distinct_count,
          count(distinct "order_payment_fee_default") = count(*) as is_unique,
          cast(min("order_payment_fee_default") as varchar) as min,
          cast(max("order_payment_fee_default") as varchar) as max,
          avg("order_payment_fee_default") as avg,
          stddev_pop("order_payment_fee_default") as std_dev_population,
          stddev_samp("order_payment_fee_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          177 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_payment_fee_actual') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_payment_fee_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_payment_fee_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_payment_fee_actual") as distinct_count,
          count(distinct "order_payment_fee_actual") = count(*) as is_unique,
          cast(min("order_payment_fee_actual") as varchar) as min,
          cast(max("order_payment_fee_actual") as varchar) as max,
          avg("order_payment_fee_actual") as avg,
          stddev_pop("order_payment_fee_actual") as std_dev_population,
          stddev_samp("order_payment_fee_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          178 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_payment_due_default') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_payment_due_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_payment_due_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_payment_due_default") as distinct_count,
          count(distinct "order_payment_due_default") = count(*) as is_unique,
          cast(min("order_payment_due_default") as varchar) as min,
          cast(max("order_payment_due_default") as varchar) as max,
          avg("order_payment_due_default") as avg,
          stddev_pop("order_payment_due_default") as std_dev_population,
          stddev_samp("order_payment_due_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          179 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_payment_due_actual') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_payment_due_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_payment_due_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_payment_due_actual") as distinct_count,
          count(distinct "order_payment_due_actual") = count(*) as is_unique,
          cast(min("order_payment_due_actual") as varchar) as min,
          cast(max("order_payment_due_actual") as varchar) as max,
          avg("order_payment_due_actual") as avg,
          stddev_pop("order_payment_due_actual") as std_dev_population,
          stddev_samp("order_payment_due_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          180 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_payment_date_autopay') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_payment_date_autopay" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_payment_date_autopay") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_payment_date_autopay") as distinct_count,
          count(distinct "order_payment_date_autopay") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          181 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_payment_method_actual') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_payment_method_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_payment_method_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_payment_method_actual") as distinct_count,
          count(distinct "order_payment_method_actual") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          182 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_payment_default_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_payment_default_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_payment_default_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_payment_default_updated_at") as distinct_count,
          count(distinct "order_payment_default_updated_at") = count(*) as is_unique,
          cast(min("order_payment_default_updated_at") as varchar) as min,
          cast(max("order_payment_default_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          183 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_payment_actual_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_payment_actual_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_payment_actual_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_payment_actual_updated_at") as distinct_count,
          count(distinct "order_payment_actual_updated_at") = count(*) as is_unique,
          cast(min("order_payment_actual_updated_at") as varchar) as min,
          cast(max("order_payment_actual_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          184 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_payment_coupon') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_payment_coupon" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_payment_coupon") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_payment_coupon") as distinct_count,
          count(distinct "order_payment_coupon") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          185 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_changed') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_changed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_changed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_changed") as distinct_count,
          count(distinct "order_date_changed") = count(*) as is_unique,
          cast(min("order_date_changed") as varchar) as min,
          cast(max("order_date_changed") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          186 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_delivered') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_delivered" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_delivered") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_delivered") as distinct_count,
          count(distinct "order_date_delivered") = count(*) as is_unique,
          cast(min("order_date_delivered") as varchar) as min,
          cast(max("order_date_delivered") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          187 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_expedited') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_expedited" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_expedited") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_expedited") as distinct_count,
          count(distinct "order_date_expedited") = count(*) as is_unique,
          cast(min("order_date_expedited") as varchar) as min,
          cast(max("order_date_expedited") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          188 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_expected') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_expected" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_expected") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_expected") as distinct_count,
          count(distinct "order_date_expected") = count(*) as is_unique,
          cast(min("order_date_expected") as varchar) as min,
          cast(max("order_date_expected") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          189 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_expected_initial') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_expected_initial" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_expected_initial") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_expected_initial") as distinct_count,
          count(distinct "order_date_expected_initial") = count(*) as is_unique,
          cast(min("order_date_expected_initial") as varchar) as min,
          cast(max("order_date_expected_initial") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          190 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_failed') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_failed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_failed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_failed") as distinct_count,
          count(distinct "order_date_failed") = count(*) as is_unique,
          cast(min("order_date_failed") as varchar) as min,
          cast(max("order_date_failed") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          191 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_date_updated') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_date_updated" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_date_updated") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_date_updated") as distinct_count,
          count(distinct "order_date_updated") = count(*) as is_unique,
          cast(min("order_date_updated") as varchar) as min,
          cast(max("order_date_updated") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          192 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_stage_wc_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_stage_wc_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_stage_wc_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_stage_wc_updated_at") as distinct_count,
          count(distinct "order_stage_wc_updated_at") = count(*) as is_unique,
          cast(min("order_stage_wc_updated_at") as varchar) as min,
          cast(max("order_stage_wc_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          193 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_add_user_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_add_user_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_add_user_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_add_user_id") as distinct_count,
          count(distinct "order_add_user_id") = count(*) as is_unique,
          cast(min("order_add_user_id") as varchar) as min,
          cast(max("order_add_user_id") as varchar) as max,
          avg("order_add_user_id") as avg,
          stddev_pop("order_add_user_id") as std_dev_population,
          stddev_samp("order_add_user_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          194 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_chg_user_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_chg_user_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_chg_user_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_chg_user_id") as distinct_count,
          count(distinct "order_chg_user_id") = count(*) as is_unique,
          cast(min("order_chg_user_id") as varchar) as min,
          cast(max("order_chg_user_id") as varchar) as max,
          avg("order_chg_user_id") as avg,
          stddev_pop("order_chg_user_id") as std_dev_population,
          stddev_samp("order_chg_user_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          195 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_shipping_speed') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_shipping_speed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_shipping_speed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_shipping_speed") as distinct_count,
          count(distinct "order_shipping_speed") = count(*) as is_unique,
          cast(min("order_shipping_speed") as varchar) as min,
          cast(max("order_shipping_speed") as varchar) as max,
          avg("order_shipping_speed") as avg,
          stddev_pop("order_shipping_speed") as std_dev_population,
          stddev_samp("order_shipping_speed") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          196 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_rx_group_removals_checked_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_rx_group_removals_checked_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_rx_group_removals_checked_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_rx_group_removals_checked_at") as distinct_count,
          count(distinct "order_rx_group_removals_checked_at") = count(*) as is_unique,
          cast(min("order_rx_group_removals_checked_at") as varchar) as min,
          cast(max("order_rx_group_removals_checked_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          197 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_rx_group_additions_checked_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_rx_group_additions_checked_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_rx_group_additions_checked_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_rx_group_additions_checked_at") as distinct_count,
          count(distinct "order_rx_group_additions_checked_at") = count(*) as is_unique,
          cast(min("order_rx_group_additions_checked_at") as varchar) as min,
          cast(max("order_rx_group_additions_checked_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          198 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_note') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_note" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_note") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_note") as distinct_count,
          count(distinct "order_note") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          199 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_rph_check') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_rph_check" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_rph_check") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_rph_check") as distinct_count,
          count(distinct "order_rph_check") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          200 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_tech_fill') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_tech_fill" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_tech_fill") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_tech_fill") as distinct_count,
          count(distinct "order_tech_fill") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          201 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_city') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_city" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_city") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_city") as distinct_count,
          count(distinct "order_city") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          202 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_state') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_state" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_state") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_state") as distinct_count,
          count(distinct "order_state") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          203 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_zip') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_zip" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_zip") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_zip") as distinct_count,
          count(distinct "order_zip") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          204 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_updated_at") as distinct_count,
          count(distinct "order_updated_at") = count(*) as is_unique,
          cast(min("order_updated_at") as varchar) as min,
          cast(max("order_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          205 as _column_position
        from source_data

        union all
      
        
        select 
          lower('order_shipped_status') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "order_shipped_status" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "order_shipped_status") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "order_shipped_status") as distinct_count,
          count(distinct "order_shipped_status") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          206 as _column_position
        from source_data

        union all
      
        
        select 
          lower('pend_group_name') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pend_group_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pend_group_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pend_group_name") as distinct_count,
          count(distinct "pend_group_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          207 as _column_position
        from source_data

        union all
      
        
        select 
          lower('pend_group_invoice_number') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pend_group_invoice_number" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pend_group_invoice_number") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pend_group_invoice_number") as distinct_count,
          count(distinct "pend_group_invoice_number") = count(*) as is_unique,
          cast(min("pend_group_invoice_number") as varchar) as min,
          cast(max("pend_group_invoice_number") as varchar) as max,
          avg("pend_group_invoice_number") as avg,
          stddev_pop("pend_group_invoice_number") as std_dev_population,
          stddev_samp("pend_group_invoice_number") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          208 as _column_position
        from source_data

        union all
      
        
        select 
          lower('pend_group_initial_date') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pend_group_initial_date" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pend_group_initial_date") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pend_group_initial_date") as distinct_count,
          count(distinct "pend_group_initial_date") = count(*) as is_unique,
          cast(min("pend_group_initial_date") as varchar) as min,
          cast(max("pend_group_initial_date") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          209 as _column_position
        from source_data

        union all
      
        
        select 
          lower('pend_group_last_date') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pend_group_last_date" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pend_group_last_date") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pend_group_last_date") as distinct_count,
          count(distinct "pend_group_last_date") = count(*) as is_unique,
          cast(min("pend_group_last_date") as varchar) as min,
          cast(max("pend_group_last_date") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          210 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_updated_at") as distinct_count,
          count(distinct "dw_updated_at") = count(*) as is_unique,
          cast(min("dw_updated_at") as varchar) as min,
          cast(max("dw_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          211 as _column_position
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
          212 as _column_position
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
          213 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_price30') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_price30" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_price30") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_price30") as distinct_count,
          count(distinct "drug_price30") = count(*) as is_unique,
          cast(min("drug_price30") as varchar) as min,
          cast(max("drug_price30") as varchar) as max,
          avg("drug_price30") as avg,
          stddev_pop("drug_price30") as std_dev_population,
          stddev_samp("drug_price30") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          214 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_price90') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_price90" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_price90") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_price90") as distinct_count,
          count(distinct "drug_price90") = count(*) as is_unique,
          cast(min("drug_price90") as varchar) as min,
          cast(max("drug_price90") as varchar) as max,
          avg("drug_price90") as avg,
          stddev_pop("drug_price90") as std_dev_population,
          stddev_samp("drug_price90") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          215 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_price_retail') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_price_retail" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_price_retail") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_price_retail") as distinct_count,
          count(distinct "drug_price_retail") = count(*) as is_unique,
          cast(min("drug_price_retail") as varchar) as min,
          cast(max("drug_price_retail") as varchar) as max,
          avg("drug_price_retail") as avg,
          stddev_pop("drug_price_retail") as std_dev_population,
          stddev_samp("drug_price_retail") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          216 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_price_goodrx') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_price_goodrx" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_price_goodrx") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_price_goodrx") as distinct_count,
          count(distinct "drug_price_goodrx") = count(*) as is_unique,
          cast(min("drug_price_goodrx") as varchar) as min,
          cast(max("drug_price_goodrx") as varchar) as max,
          avg("drug_price_goodrx") as avg,
          stddev_pop("drug_price_goodrx") as std_dev_population,
          stddev_samp("drug_price_goodrx") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          217 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_price_nadac') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_price_nadac" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_price_nadac") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_price_nadac") as distinct_count,
          count(distinct "drug_price_nadac") = count(*) as is_unique,
          cast(min("drug_price_nadac") as varchar) as min,
          cast(max("drug_price_nadac") as varchar) as max,
          avg("drug_price_nadac") as avg,
          stddev_pop("drug_price_nadac") as std_dev_population,
          stddev_samp("drug_price_nadac") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          218 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_qty_repack') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_qty_repack" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_qty_repack") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_qty_repack") as distinct_count,
          count(distinct "drug_qty_repack") = count(*) as is_unique,
          cast(min("drug_qty_repack") as varchar) as min,
          cast(max("drug_qty_repack") as varchar) as max,
          avg("drug_qty_repack") as avg,
          stddev_pop("drug_qty_repack") as std_dev_population,
          stddev_samp("drug_qty_repack") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          219 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_count_ndcs') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_count_ndcs" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_count_ndcs") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_count_ndcs") as distinct_count,
          count(distinct "drug_count_ndcs") = count(*) as is_unique,
          cast(min("drug_count_ndcs") as varchar) as min,
          cast(max("drug_count_ndcs") as varchar) as max,
          avg("drug_count_ndcs") as avg,
          stddev_pop("drug_count_ndcs") as std_dev_population,
          stddev_samp("drug_count_ndcs") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          220 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_ordered') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_ordered" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_ordered") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_ordered") as distinct_count,
          count(distinct "drug_ordered") = count(*) as is_unique,
          cast(min("drug_ordered") as varchar) as min,
          cast(max("drug_ordered") as varchar) as max,
          avg("drug_ordered") as avg,
          stddev_pop("drug_ordered") as std_dev_population,
          stddev_samp("drug_ordered") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          221 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_qty_min') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_qty_min" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_qty_min") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_qty_min") as distinct_count,
          count(distinct "drug_qty_min") = count(*) as is_unique,
          cast(min("drug_qty_min") as varchar) as min,
          cast(max("drug_qty_min") as varchar) as max,
          avg("drug_qty_min") as avg,
          stddev_pop("drug_qty_min") as std_dev_population,
          stddev_samp("drug_qty_min") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          222 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_days_min') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_days_min" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_days_min") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_days_min") as distinct_count,
          count(distinct "drug_days_min") = count(*) as is_unique,
          cast(min("drug_days_min") as varchar) as min,
          cast(max("drug_days_min") as varchar) as max,
          avg("drug_days_min") as avg,
          stddev_pop("drug_days_min") as std_dev_population,
          stddev_samp("drug_days_min") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          223 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_max_inventory') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_max_inventory" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_max_inventory") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_max_inventory") as distinct_count,
          count(distinct "drug_max_inventory") = count(*) as is_unique,
          cast(min("drug_max_inventory") as varchar) as min,
          cast(max("drug_max_inventory") as varchar) as max,
          avg("drug_max_inventory") as avg,
          stddev_pop("drug_max_inventory") as std_dev_population,
          stddev_samp("drug_max_inventory") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          224 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_message_display') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_message_display" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_message_display") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_message_display") as distinct_count,
          count(distinct "drug_message_display") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          225 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_message_verified') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_message_verified" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_message_verified") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_message_verified") as distinct_count,
          count(distinct "drug_message_verified") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          226 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_message_destroyed') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_message_destroyed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_message_destroyed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_message_destroyed") as distinct_count,
          count(distinct "drug_message_destroyed") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          227 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_generic_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_generic_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_generic_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_generic_name") as distinct_count,
          count(distinct "drug_generic_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          228 as _column_position
        from source_data

        union all
      
        
        select 
          lower('drug_price_coalesced') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "drug_price_coalesced" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "drug_price_coalesced") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "drug_price_coalesced") as distinct_count,
          count(distinct "drug_price_coalesced") = count(*) as is_unique,
          cast(min("drug_price_coalesced") as varchar) as min,
          cast(max("drug_price_coalesced") as varchar) as max,
          avg("drug_price_coalesced") as avg,
          stddev_pop("drug_price_coalesced") as std_dev_population,
          stddev_samp("drug_price_coalesced") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          229 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_drug_generic') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_drug_generic" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_drug_generic") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_drug_generic") as distinct_count,
          count(distinct "stock_live_drug_generic") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          230 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_price_per_month') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_price_per_month" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_price_per_month") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_price_per_month") as distinct_count,
          count(distinct "stock_live_price_per_month") = count(*) as is_unique,
          cast(min("stock_live_price_per_month") as varchar) as min,
          cast(max("stock_live_price_per_month") as varchar) as max,
          avg("stock_live_price_per_month") as avg,
          stddev_pop("stock_live_price_per_month") as std_dev_population,
          stddev_samp("stock_live_price_per_month") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          231 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_drug_ordered') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_drug_ordered" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_drug_ordered") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_drug_ordered") as distinct_count,
          count(distinct "stock_live_drug_ordered") = count(*) as is_unique,
          cast(min("stock_live_drug_ordered") as varchar) as min,
          cast(max("stock_live_drug_ordered") as varchar) as max,
          avg("stock_live_drug_ordered") as avg,
          stddev_pop("stock_live_drug_ordered") as std_dev_population,
          stddev_samp("stock_live_drug_ordered") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          232 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_qty_repack') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_qty_repack" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_qty_repack") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_qty_repack") as distinct_count,
          count(distinct "stock_live_qty_repack") = count(*) as is_unique,
          cast(min("stock_live_qty_repack") as varchar) as min,
          cast(max("stock_live_qty_repack") as varchar) as max,
          avg("stock_live_qty_repack") as avg,
          stddev_pop("stock_live_qty_repack") as std_dev_population,
          stddev_samp("stock_live_qty_repack") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          233 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_months_inventory') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_months_inventory" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_months_inventory") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_months_inventory") as distinct_count,
          count(distinct "stock_live_months_inventory") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          234 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_avg_inventory') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_avg_inventory" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_avg_inventory") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_avg_inventory") as distinct_count,
          count(distinct "stock_live_avg_inventory") = count(*) as is_unique,
          cast(min("stock_live_avg_inventory") as varchar) as min,
          cast(max("stock_live_avg_inventory") as varchar) as max,
          avg("stock_live_avg_inventory") as avg,
          stddev_pop("stock_live_avg_inventory") as std_dev_population,
          stddev_samp("stock_live_avg_inventory") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          235 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_last_inventory') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_last_inventory" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_last_inventory") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_last_inventory") as distinct_count,
          count(distinct "stock_live_last_inventory") = count(*) as is_unique,
          cast(min("stock_live_last_inventory") as varchar) as min,
          cast(max("stock_live_last_inventory") as varchar) as max,
          avg("stock_live_last_inventory") as avg,
          stddev_pop("stock_live_last_inventory") as std_dev_population,
          stddev_samp("stock_live_last_inventory") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          236 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_months_entered') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_months_entered" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_months_entered") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_months_entered") as distinct_count,
          count(distinct "stock_live_months_entered") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          237 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_stddev_entered') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_stddev_entered" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_stddev_entered") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_stddev_entered") as distinct_count,
          count(distinct "stock_live_stddev_entered") = count(*) as is_unique,
          cast(min("stock_live_stddev_entered") as varchar) as min,
          cast(max("stock_live_stddev_entered") as varchar) as max,
          avg("stock_live_stddev_entered") as avg,
          stddev_pop("stock_live_stddev_entered") as std_dev_population,
          stddev_samp("stock_live_stddev_entered") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          238 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_total_entered') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_total_entered" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_total_entered") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_total_entered") as distinct_count,
          count(distinct "stock_live_total_entered") = count(*) as is_unique,
          cast(min("stock_live_total_entered") as varchar) as min,
          cast(max("stock_live_total_entered") as varchar) as max,
          avg("stock_live_total_entered") as avg,
          stddev_pop("stock_live_total_entered") as std_dev_population,
          stddev_samp("stock_live_total_entered") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          239 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_months_dispensed') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_months_dispensed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_months_dispensed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_months_dispensed") as distinct_count,
          count(distinct "stock_live_months_dispensed") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          240 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_stddev_dispensed_actual') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_stddev_dispensed_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_stddev_dispensed_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_stddev_dispensed_actual") as distinct_count,
          count(distinct "stock_live_stddev_dispensed_actual") = count(*) as is_unique,
          cast(min("stock_live_stddev_dispensed_actual") as varchar) as min,
          cast(max("stock_live_stddev_dispensed_actual") as varchar) as max,
          avg("stock_live_stddev_dispensed_actual") as avg,
          stddev_pop("stock_live_stddev_dispensed_actual") as std_dev_population,
          stddev_samp("stock_live_stddev_dispensed_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          241 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_total_dispensed_actual') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_total_dispensed_actual" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_total_dispensed_actual") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_total_dispensed_actual") as distinct_count,
          count(distinct "stock_live_total_dispensed_actual") = count(*) as is_unique,
          cast(min("stock_live_total_dispensed_actual") as varchar) as min,
          cast(max("stock_live_total_dispensed_actual") as varchar) as max,
          avg("stock_live_total_dispensed_actual") as avg,
          stddev_pop("stock_live_total_dispensed_actual") as std_dev_population,
          stddev_samp("stock_live_total_dispensed_actual") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          242 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_total_dispensed_default') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_total_dispensed_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_total_dispensed_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_total_dispensed_default") as distinct_count,
          count(distinct "stock_live_total_dispensed_default") = count(*) as is_unique,
          cast(min("stock_live_total_dispensed_default") as varchar) as min,
          cast(max("stock_live_total_dispensed_default") as varchar) as max,
          avg("stock_live_total_dispensed_default") as avg,
          stddev_pop("stock_live_total_dispensed_default") as std_dev_population,
          stddev_samp("stock_live_total_dispensed_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          243 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_stddev_dispensed_default') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_stddev_dispensed_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_stddev_dispensed_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_stddev_dispensed_default") as distinct_count,
          count(distinct "stock_live_stddev_dispensed_default") = count(*) as is_unique,
          cast(min("stock_live_stddev_dispensed_default") as varchar) as min,
          cast(max("stock_live_stddev_dispensed_default") as varchar) as max,
          avg("stock_live_stddev_dispensed_default") as avg,
          stddev_pop("stock_live_stddev_dispensed_default") as std_dev_population,
          stddev_samp("stock_live_stddev_dispensed_default") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          244 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_month_interval') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_month_interval" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_month_interval") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_month_interval") as distinct_count,
          count(distinct "stock_live_month_interval") = count(*) as is_unique,
          cast(min("stock_live_month_interval") as varchar) as min,
          cast(max("stock_live_month_interval") as varchar) as max,
          avg("stock_live_month_interval") as avg,
          stddev_pop("stock_live_month_interval") as std_dev_population,
          stddev_samp("stock_live_month_interval") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          245 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_default_rxs_min') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_default_rxs_min" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_default_rxs_min") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_default_rxs_min") as distinct_count,
          count(distinct "stock_live_default_rxs_min") = count(*) as is_unique,
          cast(min("stock_live_default_rxs_min") as varchar) as min,
          cast(max("stock_live_default_rxs_min") as varchar) as max,
          avg("stock_live_default_rxs_min") as avg,
          stddev_pop("stock_live_default_rxs_min") as std_dev_population,
          stddev_samp("stock_live_default_rxs_min") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          246 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_last_inv_low_threshold') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_last_inv_low_threshold" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_last_inv_low_threshold") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_last_inv_low_threshold") as distinct_count,
          count(distinct "stock_live_last_inv_low_threshold") = count(*) as is_unique,
          cast(min("stock_live_last_inv_low_threshold") as varchar) as min,
          cast(max("stock_live_last_inv_low_threshold") as varchar) as max,
          avg("stock_live_last_inv_low_threshold") as avg,
          stddev_pop("stock_live_last_inv_low_threshold") as std_dev_population,
          stddev_samp("stock_live_last_inv_low_threshold") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          247 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_last_inv_high_threshold') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_last_inv_high_threshold" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_last_inv_high_threshold") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_last_inv_high_threshold") as distinct_count,
          count(distinct "stock_live_last_inv_high_threshold") = count(*) as is_unique,
          cast(min("stock_live_last_inv_high_threshold") as varchar) as min,
          cast(max("stock_live_last_inv_high_threshold") as varchar) as max,
          avg("stock_live_last_inv_high_threshold") as avg,
          stddev_pop("stock_live_last_inv_high_threshold") as std_dev_population,
          stddev_samp("stock_live_last_inv_high_threshold") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          248 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_last_inv_onetime_threshold') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_last_inv_onetime_threshold" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_last_inv_onetime_threshold") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_last_inv_onetime_threshold") as distinct_count,
          count(distinct "stock_live_last_inv_onetime_threshold") = count(*) as is_unique,
          cast(min("stock_live_last_inv_onetime_threshold") as varchar) as min,
          cast(max("stock_live_last_inv_onetime_threshold") as varchar) as max,
          avg("stock_live_last_inv_onetime_threshold") as avg,
          stddev_pop("stock_live_last_inv_onetime_threshold") as std_dev_population,
          stddev_samp("stock_live_last_inv_onetime_threshold") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          249 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_zlow_threshold') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_zlow_threshold" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_zlow_threshold") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_zlow_threshold") as distinct_count,
          count(distinct "stock_live_zlow_threshold") = count(*) as is_unique,
          cast(min("stock_live_zlow_threshold") as varchar) as min,
          cast(max("stock_live_zlow_threshold") as varchar) as max,
          avg("stock_live_zlow_threshold") as avg,
          stddev_pop("stock_live_zlow_threshold") as std_dev_population,
          stddev_samp("stock_live_zlow_threshold") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          250 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_zhigh_threshold') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_zhigh_threshold" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_zhigh_threshold") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_zhigh_threshold") as distinct_count,
          count(distinct "stock_live_zhigh_threshold") = count(*) as is_unique,
          cast(min("stock_live_zhigh_threshold") as varchar) as min,
          cast(max("stock_live_zhigh_threshold") as varchar) as max,
          avg("stock_live_zhigh_threshold") as avg,
          stddev_pop("stock_live_zhigh_threshold") as std_dev_population,
          stddev_samp("stock_live_zhigh_threshold") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          251 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_zscore') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_zscore" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_zscore") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_zscore") as distinct_count,
          count(distinct "stock_live_zscore") = count(*) as is_unique,
          cast(min("stock_live_zscore") as varchar) as min,
          cast(max("stock_live_zscore") as varchar) as max,
          avg("stock_live_zscore") as avg,
          stddev_pop("stock_live_zscore") as std_dev_population,
          stddev_samp("stock_live_zscore") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          252 as _column_position
        from source_data

        union all
      
        
        select 
          lower('stock_live_level') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "stock_live_level" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "stock_live_level") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "stock_live_level") as distinct_count,
          count(distinct "stock_live_level") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          253 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_id_wc') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_id_wc" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_id_wc") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_id_wc") as distinct_count,
          count(distinct "patient_id_wc") = count(*) as is_unique,
          cast(min("patient_id_wc") as varchar) as min,
          cast(max("patient_id_wc") as varchar) as max,
          avg("patient_id_wc") as avg,
          stddev_pop("patient_id_wc") as std_dev_population,
          stddev_samp("patient_id_wc") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          254 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_registered') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_registered" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_registered") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_registered") as distinct_count,
          count(distinct "patient_date_registered") = count(*) as is_unique,
          cast(min("patient_date_registered") as varchar) as min,
          cast(max("patient_date_registered") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          255 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_reviewed') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_reviewed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_reviewed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_reviewed") as distinct_count,
          count(distinct "patient_date_reviewed") = count(*) as is_unique,
          cast(min("patient_date_reviewed") as varchar) as min,
          cast(max("patient_date_reviewed") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          256 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_added') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_added" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_added") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_added") as distinct_count,
          count(distinct "patient_date_added") = count(*) as is_unique,
          cast(min("patient_date_added") as varchar) as min,
          cast(max("patient_date_added") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          257 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_changed') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_changed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_changed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_changed") as distinct_count,
          count(distinct "patient_date_changed") = count(*) as is_unique,
          cast(min("patient_date_changed") as varchar) as min,
          cast(max("patient_date_changed") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          258 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_updated') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_updated" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_updated") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_updated") as distinct_count,
          count(distinct "patient_date_updated") = count(*) as is_unique,
          cast(min("patient_date_updated") as varchar) as min,
          cast(max("patient_date_updated") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          259 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_first_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_first_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_first_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_first_name") as distinct_count,
          count(distinct "patient_first_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          260 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_last_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_last_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_last_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_last_name") as distinct_count,
          count(distinct "patient_last_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          261 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_birth_date') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_birth_date" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_birth_date") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_birth_date") as distinct_count,
          count(distinct "patient_birth_date") = count(*) as is_unique,
          cast(min("patient_birth_date") as varchar) as min,
          cast(max("patient_birth_date") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          262 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_language') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_language" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_language") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_language") as distinct_count,
          count(distinct "patient_language") = count(*) as is_unique,
          cast(min("patient_language") as varchar) as min,
          cast(max("patient_language") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          263 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_phone1') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_phone1" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_phone1") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_phone1") as distinct_count,
          count(distinct "patient_phone1") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          264 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_phone2') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_phone2" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_phone2") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_phone2") as distinct_count,
          count(distinct "patient_phone2") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          265 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_address') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_address" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_address") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_address") as distinct_count,
          count(distinct "patient_address") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          266 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_city') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_city" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_city") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_city") as distinct_count,
          count(distinct "patient_city") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          267 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_state') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_state" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_state") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_state") as distinct_count,
          count(distinct "patient_state") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          268 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_zip') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_zip" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_zip") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_zip") as distinct_count,
          count(distinct "patient_zip") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          269 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_payment_card_type') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_payment_card_type" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_payment_card_type") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_payment_card_type") as distinct_count,
          count(distinct "patient_payment_card_type") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          270 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_payment_card_last4') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_payment_card_last4" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_payment_card_last4") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_payment_card_last4") as distinct_count,
          count(distinct "patient_payment_card_last4") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          271 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_payment_card_date_expired') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_payment_card_date_expired" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_payment_card_date_expired") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_payment_card_date_expired") as distinct_count,
          count(distinct "patient_payment_card_date_expired") = count(*) as is_unique,
          cast(min("patient_payment_card_date_expired") as varchar) as min,
          cast(max("patient_payment_card_date_expired") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          272 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_payment_card_autopay') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_payment_card_autopay" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_payment_card_autopay") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_payment_card_autopay") as distinct_count,
          count(distinct "patient_payment_card_autopay") = count(*) as is_unique,
          cast(min("patient_payment_card_autopay") as varchar) as min,
          cast(max("patient_payment_card_autopay") as varchar) as max,
          avg("patient_payment_card_autopay") as avg,
          stddev_pop("patient_payment_card_autopay") as std_dev_population,
          stddev_samp("patient_payment_card_autopay") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          273 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_payment_method_default') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_payment_method_default" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_payment_method_default") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_payment_method_default") as distinct_count,
          count(distinct "patient_payment_method_default") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          274 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_first_rx_received') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_first_rx_received" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_first_rx_received") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_first_rx_received") as distinct_count,
          count(distinct "patient_date_first_rx_received") = count(*) as is_unique,
          cast(min("patient_date_first_rx_received") as varchar) as min,
          cast(max("patient_date_first_rx_received") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          275 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_first_dispensed') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_first_dispensed" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_first_dispensed") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_first_dispensed") as distinct_count,
          count(distinct "patient_date_first_dispensed") = count(*) as is_unique,
          cast(min("patient_date_first_dispensed") as varchar) as min,
          cast(max("patient_date_first_dispensed") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          276 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_date_first_expected_by') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_date_first_expected_by" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_date_first_expected_by") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_date_first_expected_by") as distinct_count,
          count(distinct "patient_date_first_expected_by") = count(*) as is_unique,
          cast(min("patient_date_first_expected_by") as varchar) as min,
          cast(max("patient_date_first_expected_by") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          277 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_refills_used') as column_name,
          nullif(lower('numeric'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_refills_used" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_refills_used") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_refills_used") as distinct_count,
          count(distinct "patient_refills_used") = count(*) as is_unique,
          cast(min("patient_refills_used") as varchar) as min,
          cast(max("patient_refills_used") as varchar) as max,
          avg("patient_refills_used") as avg,
          stddev_pop("patient_refills_used") as std_dev_population,
          stddev_samp("patient_refills_used") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          278 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_email') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_email" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_email") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_email") as distinct_count,
          count(distinct "patient_email") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          279 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_autofill') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_autofill" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_autofill") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_autofill") as distinct_count,
          count(distinct "patient_autofill") = count(*) as is_unique,
          cast(min("patient_autofill") as varchar) as min,
          cast(max("patient_autofill") as varchar) as max,
          avg("patient_autofill") as avg,
          stddev_pop("patient_autofill") as std_dev_population,
          stddev_samp("patient_autofill") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          280 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_initial_invoice_number') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_initial_invoice_number" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_initial_invoice_number") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_initial_invoice_number") as distinct_count,
          count(distinct "patient_initial_invoice_number") = count(*) as is_unique,
          cast(min("patient_initial_invoice_number") as varchar) as min,
          cast(max("patient_initial_invoice_number") as varchar) as max,
          avg("patient_initial_invoice_number") as avg,
          stddev_pop("patient_initial_invoice_number") as std_dev_population,
          stddev_samp("patient_initial_invoice_number") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          281 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_note') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_note" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_note") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_note") as distinct_count,
          count(distinct "patient_note") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          282 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_allergies_none') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_allergies_none" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_allergies_none") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_allergies_none") as distinct_count,
          count(distinct "patient_allergies_none") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          283 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_allergies_cephalosporins') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_allergies_cephalosporins" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_allergies_cephalosporins") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_allergies_cephalosporins") as distinct_count,
          count(distinct "patient_allergies_cephalosporins") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          284 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_allergies_sulfa') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_allergies_sulfa" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_allergies_sulfa") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_allergies_sulfa") as distinct_count,
          count(distinct "patient_allergies_sulfa") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          285 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_allergies_aspirin') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_allergies_aspirin" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_allergies_aspirin") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_allergies_aspirin") as distinct_count,
          count(distinct "patient_allergies_aspirin") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          286 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_allergies_penicillin') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_allergies_penicillin" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_allergies_penicillin") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_allergies_penicillin") as distinct_count,
          count(distinct "patient_allergies_penicillin") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          287 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_allergies_erythromycin') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_allergies_erythromycin" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_allergies_erythromycin") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_allergies_erythromycin") as distinct_count,
          count(distinct "patient_allergies_erythromycin") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          288 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_allergies_codeine') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_allergies_codeine" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_allergies_codeine") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_allergies_codeine") as distinct_count,
          count(distinct "patient_allergies_codeine") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          289 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_allergies_nsaids') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_allergies_nsaids" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_allergies_nsaids") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_allergies_nsaids") as distinct_count,
          count(distinct "patient_allergies_nsaids") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          290 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_allergies_salicylates') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_allergies_salicylates" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_allergies_salicylates") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_allergies_salicylates") as distinct_count,
          count(distinct "patient_allergies_salicylates") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          291 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_allergies_azithromycin') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_allergies_azithromycin" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_allergies_azithromycin") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_allergies_azithromycin") as distinct_count,
          count(distinct "patient_allergies_azithromycin") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          292 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_allergies_amoxicillin') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_allergies_amoxicillin" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_allergies_amoxicillin") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_allergies_amoxicillin") as distinct_count,
          count(distinct "patient_allergies_amoxicillin") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          293 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_allergies_tetracycline') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_allergies_tetracycline" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_allergies_tetracycline") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_allergies_tetracycline") as distinct_count,
          count(distinct "patient_allergies_tetracycline") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          294 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_allergies_other') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_allergies_other" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_allergies_other") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_allergies_other") as distinct_count,
          count(distinct "patient_allergies_other") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          295 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_medications_other') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_medications_other" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_medications_other") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_medications_other") as distinct_count,
          count(distinct "patient_medications_other") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          296 as _column_position
        from source_data

        union all
      
        
        select 
          lower('pharmacy_npi') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pharmacy_npi" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pharmacy_npi") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pharmacy_npi") as distinct_count,
          count(distinct "pharmacy_npi") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          297 as _column_position
        from source_data

        union all
      
        
        select 
          lower('pharmacy_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pharmacy_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pharmacy_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pharmacy_name") as distinct_count,
          count(distinct "pharmacy_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          298 as _column_position
        from source_data

        union all
      
        
        select 
          lower('pharmacy_phone') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pharmacy_phone" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pharmacy_phone") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pharmacy_phone") as distinct_count,
          count(distinct "pharmacy_phone") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          299 as _column_position
        from source_data

        union all
      
        
        select 
          lower('pharmacy_fax') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pharmacy_fax" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pharmacy_fax") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pharmacy_fax") as distinct_count,
          count(distinct "pharmacy_fax") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          300 as _column_position
        from source_data

        union all
      
        
        select 
          lower('pharmacy_address') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "pharmacy_address" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "pharmacy_address") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "pharmacy_address") as distinct_count,
          count(distinct "pharmacy_address") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          301 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_inactive') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_inactive" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_inactive") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_inactive") as distinct_count,
          count(distinct "patient_inactive") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          302 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_payment_coupon') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_payment_coupon" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_payment_coupon") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_payment_coupon") as distinct_count,
          count(distinct "patient_payment_coupon") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          303 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_tracking_coupon') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_tracking_coupon" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_tracking_coupon") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_tracking_coupon") as distinct_count,
          count(distinct "patient_tracking_coupon") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          304 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_patient_deleted') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_patient_deleted" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_patient_deleted") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_patient_deleted") as distinct_count,
          count(distinct "patient_patient_deleted") = count(*) as is_unique,
          cast(min("patient_patient_deleted") as varchar) as min,
          cast(max("patient_patient_deleted") as varchar) as max,
          avg("patient_patient_deleted") as avg,
          stddev_pop("patient_patient_deleted") as std_dev_population,
          stddev_samp("patient_patient_deleted") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          305 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_third_party_id') as column_name,
          nullif(lower('bigint'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_third_party_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_third_party_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_third_party_id") as distinct_count,
          count(distinct "patient_third_party_id") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          306 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_terms_viewed_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_terms_viewed_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_terms_viewed_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_terms_viewed_at") as distinct_count,
          count(distinct "patient_terms_viewed_at") = count(*) as is_unique,
          cast(min("patient_terms_viewed_at") as varchar) as min,
          cast(max("patient_terms_viewed_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          307 as _column_position
        from source_data

        union all
      
        
        select 
          lower('patient_terms_accepted') as column_name,
          nullif(lower('boolean'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "patient_terms_accepted" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "patient_terms_accepted") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "patient_terms_accepted") as distinct_count,
          count(distinct "patient_terms_accepted") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          308 as _column_position
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
          309 as _column_position
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
          310 as _column_position
        from source_data

        union all
      
        
        select 
          lower('provider_verified') as column_name,
          nullif(lower('boolean'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "provider_verified" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "provider_verified") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "provider_verified") as distinct_count,
          count(distinct "provider_verified") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          311 as _column_position
        from source_data

        union all
      
        
        select 
          lower('providers_npi') as column_name,
          nullif(lower('character varying'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "providers_npi" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "providers_npi") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "providers_npi") as distinct_count,
          count(distinct "providers_npi") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          312 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_provider_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_provider_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_provider_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_provider_id") as distinct_count,
          count(distinct "dw_provider_id") = count(*) as is_unique,
          cast(min("dw_provider_id") as varchar) as min,
          cast(max("dw_provider_id") as varchar) as max,
          avg("dw_provider_id") as avg,
          stddev_pop("dw_provider_id") as std_dev_population,
          stddev_samp("dw_provider_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          313 as _column_position
        from source_data

        union all
      
        
        select 
          lower('provider_first_rx_sent_date') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "provider_first_rx_sent_date" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "provider_first_rx_sent_date") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "provider_first_rx_sent_date") as distinct_count,
          count(distinct "provider_first_rx_sent_date") = count(*) as is_unique,
          cast(min("provider_first_rx_sent_date") as varchar) as min,
          cast(max("provider_first_rx_sent_date") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          314 as _column_position
        from source_data

        union all
      
        
        select 
          lower('provider_last_rx_sent_date') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "provider_last_rx_sent_date" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "provider_last_rx_sent_date") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "provider_last_rx_sent_date") as distinct_count,
          count(distinct "provider_last_rx_sent_date") = count(*) as is_unique,
          cast(min("provider_last_rx_sent_date") as varchar) as min,
          cast(max("provider_last_rx_sent_date") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          315 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_provider_npi') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_provider_npi" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_provider_npi") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_provider_npi") as distinct_count,
          count(distinct "dw_provider_npi") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          316 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_provider_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_provider_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_provider_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_provider_name") as distinct_count,
          count(distinct "dw_provider_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          317 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_provider_phone') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_provider_phone" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_provider_phone") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_provider_phone") as distinct_count,
          count(distinct "dw_provider_phone") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          318 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_provider_id_sf') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_provider_id_sf" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_provider_id_sf") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_provider_id_sf") as distinct_count,
          count(distinct "dw_provider_id_sf") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          319 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_provider_default_clinic') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_provider_default_clinic" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_provider_default_clinic") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_provider_default_clinic") as distinct_count,
          count(distinct "dw_provider_default_clinic") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          320 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_provider_default_clinic_imputed_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_provider_default_clinic_imputed_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_provider_default_clinic_imputed_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_provider_default_clinic_imputed_at") as distinct_count,
          count(distinct "dw_provider_default_clinic_imputed_at") = count(*) as is_unique,
          cast(min("dw_provider_default_clinic_imputed_at") as varchar) as min,
          cast(max("dw_provider_default_clinic_imputed_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          321 as _column_position
        from source_data

        union all
      
        
        select 
          lower('clinic_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_id") as distinct_count,
          count(distinct "clinic_id") = count(*) as is_unique,
          cast(min("clinic_id") as varchar) as min,
          cast(max("clinic_id") as varchar) as max,
          avg("clinic_id") as avg,
          stddev_pop("clinic_id") as std_dev_population,
          stddev_samp("clinic_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          322 as _column_position
        from source_data

        union all
      
        
        select 
          lower('clinic_name_cp') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_name_cp" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_name_cp") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_name_cp") as distinct_count,
          count(distinct "clinic_name_cp") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          323 as _column_position
        from source_data

        union all
      
        
        select 
          lower('clinic_rx_date_added_first') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_rx_date_added_first" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_rx_date_added_first") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_rx_date_added_first") as distinct_count,
          count(distinct "clinic_rx_date_added_first") = count(*) as is_unique,
          cast(min("clinic_rx_date_added_first") as varchar) as min,
          cast(max("clinic_rx_date_added_first") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          324 as _column_position
        from source_data

        union all
      
        
        select 
          lower('clinic_rx_date_added_last') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_rx_date_added_last" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_rx_date_added_last") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_rx_date_added_last") as distinct_count,
          count(distinct "clinic_rx_date_added_last") = count(*) as is_unique,
          cast(min("clinic_rx_date_added_last") as varchar) as min,
          cast(max("clinic_rx_date_added_last") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          325 as _column_position
        from source_data

        union all
      
        
        select 
          lower('clinic_created_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_created_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_created_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_created_at") as distinct_count,
          count(distinct "clinic_created_at") = count(*) as is_unique,
          cast(min("clinic_created_at") as varchar) as min,
          cast(max("clinic_created_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          326 as _column_position
        from source_data

        union all
      
        
        select 
          lower('clinic_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "clinic_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "clinic_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "clinic_updated_at") as distinct_count,
          count(distinct "clinic_updated_at") = count(*) as is_unique,
          cast(min("clinic_updated_at") as varchar) as min,
          cast(max("clinic_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          327 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_id") as distinct_count,
          count(distinct "dw_clinic_id") = count(*) as is_unique,
          cast(min("dw_clinic_id") as varchar) as min,
          cast(max("dw_clinic_id") as varchar) as max,
          avg("dw_clinic_id") as avg,
          stddev_pop("dw_clinic_id") as std_dev_population,
          stddev_samp("dw_clinic_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          328 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_group_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_group_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_group_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_group_id") as distinct_count,
          count(distinct "dw_clinic_group_id") = count(*) as is_unique,
          cast(min("dw_clinic_group_id") as varchar) as min,
          cast(max("dw_clinic_group_id") as varchar) as max,
          avg("dw_clinic_group_id") as avg,
          stddev_pop("dw_clinic_group_id") as std_dev_population,
          stddev_samp("dw_clinic_group_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          329 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_name") as distinct_count,
          count(distinct "dw_clinic_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          330 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_address') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_address" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_address") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_address") as distinct_count,
          count(distinct "dw_clinic_address") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          331 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_street') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_street" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_street") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_street") as distinct_count,
          count(distinct "dw_clinic_street") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          332 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_city') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_city" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_city") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_city") as distinct_count,
          count(distinct "dw_clinic_city") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          333 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_state') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_state" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_state") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_state") as distinct_count,
          count(distinct "dw_clinic_state") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          334 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_zip') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_zip" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_zip") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_zip") as distinct_count,
          count(distinct "dw_clinic_zip") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          335 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_phone') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_phone" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_phone") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_phone") as distinct_count,
          count(distinct "dw_clinic_phone") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          336 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_id_sf') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_id_sf" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_id_sf") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_id_sf") as distinct_count,
          count(distinct "dw_clinic_id_sf") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          337 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_created_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_created_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_created_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_created_at") as distinct_count,
          count(distinct "dw_clinic_created_at") = count(*) as is_unique,
          cast(min("dw_clinic_created_at") as varchar) as min,
          cast(max("dw_clinic_created_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          338 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_updated_at") as distinct_count,
          count(distinct "dw_clinic_updated_at") = count(*) as is_unique,
          cast(min("dw_clinic_updated_at") as varchar) as min,
          cast(max("dw_clinic_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          339 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_groups_id') as column_name,
          nullif(lower('integer'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_groups_id" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_groups_id") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_groups_id") as distinct_count,
          count(distinct "dw_clinic_groups_id") = count(*) as is_unique,
          cast(min("dw_clinic_groups_id") as varchar) as min,
          cast(max("dw_clinic_groups_id") as varchar) as max,
          avg("dw_clinic_groups_id") as avg,
          stddev_pop("dw_clinic_groups_id") as std_dev_population,
          stddev_samp("dw_clinic_groups_id") as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          340 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_group_name') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_group_name" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_group_name") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_group_name") as distinct_count,
          count(distinct "dw_clinic_group_name") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          341 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_group_id_sf') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_group_id_sf" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_group_id_sf") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_group_id_sf") as distinct_count,
          count(distinct "dw_clinic_group_id_sf") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          342 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_group_domain') as column_name,
          nullif(lower('text'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_group_domain" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_group_domain") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_group_domain") as distinct_count,
          count(distinct "dw_clinic_group_domain") = count(*) as is_unique,
          null as min,
          null as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          343 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_groups_created_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_groups_created_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_groups_created_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_groups_created_at") as distinct_count,
          count(distinct "dw_clinic_groups_created_at") = count(*) as is_unique,
          cast(min("dw_clinic_groups_created_at") as varchar) as min,
          cast(max("dw_clinic_groups_created_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          344 as _column_position
        from source_data

        union all
      
        
        select 
          lower('dw_clinic_groups_updated_at') as column_name,
          nullif(lower('timestamp without time zone'), '') as data_type,
          cast(count(*) as numeric) as row_count,
          sum(case when "dw_clinic_groups_updated_at" is null then 0 else 1 end) / cast(count(*) as numeric) as not_null_proportion,
          count(distinct "dw_clinic_groups_updated_at") / cast(count(*) as numeric) as distinct_proportion,
          count(distinct "dw_clinic_groups_updated_at") as distinct_count,
          count(distinct "dw_clinic_groups_updated_at") = count(*) as is_unique,
          cast(min("dw_clinic_groups_updated_at") as varchar) as min,
          cast(max("dw_clinic_groups_updated_at") as varchar) as max,
          cast(null as numeric) as avg,
          cast(null as numeric) as std_dev_population,
          cast(null as numeric) as std_dev_sample,
          cast(current_timestamp as varchar) as profiled_at,
          345 as _column_position
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
  