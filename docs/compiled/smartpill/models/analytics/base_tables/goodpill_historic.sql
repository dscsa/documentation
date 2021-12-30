with gph as (


        (
            select

                cast('"datawarehouse".analytics."orders_historic"' as 
    varchar
) as _dbt_source_relation,
                
                    cast("invoice_number" as integer) as "invoice_number" ,
                    cast("event_name" as text) as "event_name" ,
                    cast("event_date" as timestamp without time zone) as "event_date" ,
                    cast("patient_id_cp" as integer) as "patient_id_cp" ,
                    cast("count_items" as integer) as "count_items" ,
                    cast("count_filled" as integer) as "count_filled" ,
                    cast("count_nofill" as integer) as "count_nofill" ,
                    cast("order_source" as character varying(80)) as "order_source" ,
                    cast("order_stage_cp" as character varying(80)) as "order_stage_cp" ,
                    cast("order_status" as character varying(80)) as "order_status" ,
                    cast("invoice_doc_id" as character varying(80)) as "invoice_doc_id" ,
                    cast("tracking_number" as character varying(80)) as "tracking_number" ,
                    cast("payment_total_default" as integer) as "payment_total_default" ,
                    cast("payment_total_actual" as integer) as "payment_total_actual" ,
                    cast("payment_fee_default" as integer) as "payment_fee_default" ,
                    cast("payment_fee_actual" as integer) as "payment_fee_actual" ,
                    cast("payment_due_default" as integer) as "payment_due_default" ,
                    cast("payment_due_actual" as integer) as "payment_due_actual" ,
                    cast("payment_date_autopay" as character varying(80)) as "payment_date_autopay" ,
                    cast("payment_method_actual" as character varying(80)) as "payment_method_actual" ,
                    cast("coupon_lines" as character varying(255)) as "coupon_lines" ,
                    cast("order_note" as text) as "order_note" ,
                    cast("rph_check" as character varying(5)) as "rph_check" ,
                    cast("tech_fill" as character varying(5)) as "tech_fill" ,
                    cast("_airbyte_emitted_at" as timestamp with time zone) as "_airbyte_emitted_at" ,
                    cast("_airbyte_ab_id" as character varying(256)) as "_airbyte_ab_id" ,
                    cast("_ab_cdc_updated_at" as timestamp without time zone) as "_ab_cdc_updated_at" ,
                    cast("_airbyte_source" as text) as "_airbyte_source" ,
                    cast("unique_event_id" as text) as "unique_event_id" ,
                    cast("location_id" as character varying(256)) as "location_id" ,
                    cast(null as integer) as "rx_number" ,
                    cast(null as character varying(255)) as "drug_generic" ,
                    cast(null as character varying(255)) as "clinic_name" ,
                    cast(null as character varying(255)) as "provider_npi" ,
                    cast(null as integer) as "is_refill" ,
                    cast(null as integer) as "rx_autofill" ,
                    cast(null as numeric(6,3)) as "sig_qty_per_day" ,
                    cast(null as character varying(80)) as "rx_message_key" ,
                    cast(null as integer) as "max_gsn" ,
                    cast(null as character varying(255)) as "drug_gsns" ,
                    cast(null as numeric(5,2)) as "refills_total" ,
                    cast(null as numeric(5,2)) as "refills_original" ,
                    cast(null as numeric(5,2)) as "refills_left" ,
                    cast(null as date) as "refill_date_first" ,
                    cast(null as date) as "refill_date_last" ,
                    cast(null as date) as "rx_date_expired" ,
                    cast(null as timestamp without time zone) as "rx_date_changed" ,
                    cast(null as numeric(10,3)) as "qty_left" ,
                    cast(null as numeric(10,3)) as "qty_original" ,
                    cast(null as character varying(255)) as "sig_actual" ,
                    cast(null as character varying(255)) as "sig_initial" ,
                    cast(null as character varying(255)) as "sig_clean" ,
                    cast(null as numeric(10,3)) as "sig_qty" ,
                    cast(null as integer) as "sig_days" ,
                    cast(null as numeric(10,3)) as "sig_qty_per_day_actual" ,
                    cast(null as numeric(10,3)) as "sig_v2_qty" ,
                    cast(null as integer) as "sig_v2_days" ,
                    cast(null as numeric(10,3)) as "sig_v2_qty_per_day" ,
                    cast(null as character varying(255)) as "sig_v2_unit" ,
                    cast(null as numeric(10,3)) as "sig_v2_conf_score" ,
                    cast(null as character varying(255)) as "sig_v2_dosages" ,
                    cast(null as character varying(255)) as "sig_v2_scores" ,
                    cast(null as character varying(255)) as "sig_v2_frequencies" ,
                    cast(null as character varying(255)) as "sig_v2_durations" ,
                    cast(null as date) as "refill_date_next" ,
                    cast(null as date) as "refill_date_manual" ,
                    cast(null as date) as "refill_date_default" ,
                    cast(null as numeric(11,3)) as "qty_total" ,
                    cast(null as character varying(80)) as "rx_source" ,
                    cast(null as character varying(80)) as "rx_transfer" ,
                    cast(null as character varying(255)) as "groups" ,
                    cast(null as integer) as "rx_dispensed_id" ,
                    cast(null as character varying(80)) as "stock_level_initial" ,
                    cast(null as character varying(255)) as "rx_message_keys_initial" ,
                    cast(null as integer) as "patient_autofill_initial" ,
                    cast(null as integer) as "rx_autofill_initial" ,
                    cast(null as character varying(255)) as "rx_numbers_initial" ,
                    cast(null as numeric(6,3)) as "zscore_initial" ,
                    cast(null as numeric(5,2)) as "refills_dispensed_default" ,
                    cast(null as numeric(5,2)) as "refills_dispensed_actual" ,
                    cast(null as integer) as "days_dispensed_default" ,
                    cast(null as integer) as "days_dispensed_actual" ,
                    cast(null as numeric(10,3)) as "qty_dispensed_default" ,
                    cast(null as numeric(10,3)) as "qty_dispensed_actual" ,
                    cast(null as numeric(5,2)) as "price_dispensed_default" ,
                    cast(null as numeric(5,2)) as "price_dispensed_actual" ,
                    cast(null as numeric(10,3)) as "qty_pended_total" ,
                    cast(null as numeric(10,3)) as "qty_pended_repacks" ,
                    cast(null as integer) as "count_pended_total" ,
                    cast(null as integer) as "count_pended_repacks" ,
                    cast(null as character varying(255)) as "item_message_keys" ,
                    cast(null as character varying(255)) as "item_message_text" ,
                    cast(null as character varying(80)) as "item_type" ,
                    cast(null as character varying(80)) as "item_added_by" ,
                    cast(null as timestamp without time zone) as "item_date_added" ,
                    cast(null as timestamp without time zone) as "refill_target_date" ,
                    cast(null as integer) as "refill_target_days" ,
                    cast(null as character varying(255)) as "refill_target_rxs" 

            from "datawarehouse".analytics."orders_historic"
        )

        union all
        

        (
            select

                cast('"datawarehouse".analytics."rxs_historic"' as 
    varchar
) as _dbt_source_relation,
                
                    cast(null as integer) as "invoice_number" ,
                    cast("event_name" as text) as "event_name" ,
                    cast("event_date" as timestamp without time zone) as "event_date" ,
                    cast("patient_id_cp" as integer) as "patient_id_cp" ,
                    cast(null as integer) as "count_items" ,
                    cast(null as integer) as "count_filled" ,
                    cast(null as integer) as "count_nofill" ,
                    cast(null as character varying(80)) as "order_source" ,
                    cast(null as character varying(80)) as "order_stage_cp" ,
                    cast(null as character varying(80)) as "order_status" ,
                    cast(null as character varying(80)) as "invoice_doc_id" ,
                    cast(null as character varying(80)) as "tracking_number" ,
                    cast(null as integer) as "payment_total_default" ,
                    cast(null as integer) as "payment_total_actual" ,
                    cast(null as integer) as "payment_fee_default" ,
                    cast(null as integer) as "payment_fee_actual" ,
                    cast(null as integer) as "payment_due_default" ,
                    cast(null as integer) as "payment_due_actual" ,
                    cast(null as character varying(80)) as "payment_date_autopay" ,
                    cast(null as character varying(80)) as "payment_method_actual" ,
                    cast(null as character varying(255)) as "coupon_lines" ,
                    cast(null as text) as "order_note" ,
                    cast(null as character varying(5)) as "rph_check" ,
                    cast(null as character varying(5)) as "tech_fill" ,
                    cast("_airbyte_emitted_at" as timestamp with time zone) as "_airbyte_emitted_at" ,
                    cast("_airbyte_ab_id" as character varying(256)) as "_airbyte_ab_id" ,
                    cast("_ab_cdc_updated_at" as timestamp without time zone) as "_ab_cdc_updated_at" ,
                    cast("_airbyte_source" as text) as "_airbyte_source" ,
                    cast("unique_event_id" as text) as "unique_event_id" ,
                    cast(null as character varying(256)) as "location_id" ,
                    cast("rx_number" as integer) as "rx_number" ,
                    cast("drug_generic" as character varying(255)) as "drug_generic" ,
                    cast("clinic_name" as character varying(255)) as "clinic_name" ,
                    cast("provider_npi" as character varying(255)) as "provider_npi" ,
                    cast("is_refill" as integer) as "is_refill" ,
                    cast("rx_autofill" as integer) as "rx_autofill" ,
                    cast("sig_qty_per_day" as numeric(6,3)) as "sig_qty_per_day" ,
                    cast("rx_message_key" as character varying(80)) as "rx_message_key" ,
                    cast("max_gsn" as integer) as "max_gsn" ,
                    cast("drug_gsns" as character varying(255)) as "drug_gsns" ,
                    cast("refills_total" as numeric(5,2)) as "refills_total" ,
                    cast("refills_original" as numeric(5,2)) as "refills_original" ,
                    cast("refills_left" as numeric(5,2)) as "refills_left" ,
                    cast("refill_date_first" as date) as "refill_date_first" ,
                    cast("refill_date_last" as date) as "refill_date_last" ,
                    cast("rx_date_expired" as date) as "rx_date_expired" ,
                    cast("rx_date_changed" as timestamp without time zone) as "rx_date_changed" ,
                    cast("qty_left" as numeric(10,3)) as "qty_left" ,
                    cast("qty_original" as numeric(10,3)) as "qty_original" ,
                    cast("sig_actual" as character varying(255)) as "sig_actual" ,
                    cast("sig_initial" as character varying(255)) as "sig_initial" ,
                    cast("sig_clean" as character varying(255)) as "sig_clean" ,
                    cast("sig_qty" as numeric(10,3)) as "sig_qty" ,
                    cast("sig_days" as integer) as "sig_days" ,
                    cast("sig_qty_per_day_actual" as numeric(10,3)) as "sig_qty_per_day_actual" ,
                    cast("sig_v2_qty" as numeric(10,3)) as "sig_v2_qty" ,
                    cast("sig_v2_days" as integer) as "sig_v2_days" ,
                    cast("sig_v2_qty_per_day" as numeric(10,3)) as "sig_v2_qty_per_day" ,
                    cast("sig_v2_unit" as character varying(255)) as "sig_v2_unit" ,
                    cast("sig_v2_conf_score" as numeric(10,3)) as "sig_v2_conf_score" ,
                    cast("sig_v2_dosages" as character varying(255)) as "sig_v2_dosages" ,
                    cast("sig_v2_scores" as character varying(255)) as "sig_v2_scores" ,
                    cast("sig_v2_frequencies" as character varying(255)) as "sig_v2_frequencies" ,
                    cast("sig_v2_durations" as character varying(255)) as "sig_v2_durations" ,
                    cast("refill_date_next" as date) as "refill_date_next" ,
                    cast("refill_date_manual" as date) as "refill_date_manual" ,
                    cast("refill_date_default" as date) as "refill_date_default" ,
                    cast("qty_total" as numeric(11,3)) as "qty_total" ,
                    cast("rx_source" as character varying(80)) as "rx_source" ,
                    cast("rx_transfer" as character varying(80)) as "rx_transfer" ,
                    cast(null as character varying(255)) as "groups" ,
                    cast(null as integer) as "rx_dispensed_id" ,
                    cast(null as character varying(80)) as "stock_level_initial" ,
                    cast(null as character varying(255)) as "rx_message_keys_initial" ,
                    cast(null as integer) as "patient_autofill_initial" ,
                    cast(null as integer) as "rx_autofill_initial" ,
                    cast(null as character varying(255)) as "rx_numbers_initial" ,
                    cast(null as numeric(6,3)) as "zscore_initial" ,
                    cast(null as numeric(5,2)) as "refills_dispensed_default" ,
                    cast(null as numeric(5,2)) as "refills_dispensed_actual" ,
                    cast(null as integer) as "days_dispensed_default" ,
                    cast(null as integer) as "days_dispensed_actual" ,
                    cast(null as numeric(10,3)) as "qty_dispensed_default" ,
                    cast(null as numeric(10,3)) as "qty_dispensed_actual" ,
                    cast(null as numeric(5,2)) as "price_dispensed_default" ,
                    cast(null as numeric(5,2)) as "price_dispensed_actual" ,
                    cast(null as numeric(10,3)) as "qty_pended_total" ,
                    cast(null as numeric(10,3)) as "qty_pended_repacks" ,
                    cast(null as integer) as "count_pended_total" ,
                    cast(null as integer) as "count_pended_repacks" ,
                    cast(null as character varying(255)) as "item_message_keys" ,
                    cast(null as character varying(255)) as "item_message_text" ,
                    cast(null as character varying(80)) as "item_type" ,
                    cast(null as character varying(80)) as "item_added_by" ,
                    cast(null as timestamp without time zone) as "item_date_added" ,
                    cast(null as timestamp without time zone) as "refill_target_date" ,
                    cast(null as integer) as "refill_target_days" ,
                    cast(null as character varying(255)) as "refill_target_rxs" 

            from "datawarehouse".analytics."rxs_historic"
        )

        union all
        

        (
            select

                cast('"datawarehouse".analytics."order_items_historic"' as 
    varchar
) as _dbt_source_relation,
                
                    cast("invoice_number" as integer) as "invoice_number" ,
                    cast("event_name" as text) as "event_name" ,
                    cast("event_date" as timestamp without time zone) as "event_date" ,
                    cast("patient_id_cp" as integer) as "patient_id_cp" ,
                    cast(null as integer) as "count_items" ,
                    cast(null as integer) as "count_filled" ,
                    cast(null as integer) as "count_nofill" ,
                    cast(null as character varying(80)) as "order_source" ,
                    cast(null as character varying(80)) as "order_stage_cp" ,
                    cast(null as character varying(80)) as "order_status" ,
                    cast(null as character varying(80)) as "invoice_doc_id" ,
                    cast(null as character varying(80)) as "tracking_number" ,
                    cast(null as integer) as "payment_total_default" ,
                    cast(null as integer) as "payment_total_actual" ,
                    cast(null as integer) as "payment_fee_default" ,
                    cast(null as integer) as "payment_fee_actual" ,
                    cast(null as integer) as "payment_due_default" ,
                    cast(null as integer) as "payment_due_actual" ,
                    cast(null as character varying(80)) as "payment_date_autopay" ,
                    cast(null as character varying(80)) as "payment_method_actual" ,
                    cast(null as character varying(255)) as "coupon_lines" ,
                    cast(null as text) as "order_note" ,
                    cast(null as character varying(5)) as "rph_check" ,
                    cast(null as character varying(5)) as "tech_fill" ,
                    cast("_airbyte_emitted_at" as timestamp with time zone) as "_airbyte_emitted_at" ,
                    cast("_airbyte_ab_id" as character varying(256)) as "_airbyte_ab_id" ,
                    cast("_ab_cdc_updated_at" as timestamp without time zone) as "_ab_cdc_updated_at" ,
                    cast("_airbyte_source" as text) as "_airbyte_source" ,
                    cast("unique_event_id" as text) as "unique_event_id" ,
                    cast(null as character varying(256)) as "location_id" ,
                    cast("rx_number" as integer) as "rx_number" ,
                    cast(null as character varying(255)) as "drug_generic" ,
                    cast(null as character varying(255)) as "clinic_name" ,
                    cast(null as character varying(255)) as "provider_npi" ,
                    cast(null as integer) as "is_refill" ,
                    cast(null as integer) as "rx_autofill" ,
                    cast(null as numeric(6,3)) as "sig_qty_per_day" ,
                    cast(null as character varying(80)) as "rx_message_key" ,
                    cast(null as integer) as "max_gsn" ,
                    cast(null as character varying(255)) as "drug_gsns" ,
                    cast(null as numeric(5,2)) as "refills_total" ,
                    cast(null as numeric(5,2)) as "refills_original" ,
                    cast(null as numeric(5,2)) as "refills_left" ,
                    cast(null as date) as "refill_date_first" ,
                    cast("refill_date_last" as date) as "refill_date_last" ,
                    cast(null as date) as "rx_date_expired" ,
                    cast(null as timestamp without time zone) as "rx_date_changed" ,
                    cast(null as numeric(10,3)) as "qty_left" ,
                    cast(null as numeric(10,3)) as "qty_original" ,
                    cast(null as character varying(255)) as "sig_actual" ,
                    cast(null as character varying(255)) as "sig_initial" ,
                    cast(null as character varying(255)) as "sig_clean" ,
                    cast(null as numeric(10,3)) as "sig_qty" ,
                    cast(null as integer) as "sig_days" ,
                    cast(null as numeric(10,3)) as "sig_qty_per_day_actual" ,
                    cast(null as numeric(10,3)) as "sig_v2_qty" ,
                    cast(null as integer) as "sig_v2_days" ,
                    cast(null as numeric(10,3)) as "sig_v2_qty_per_day" ,
                    cast(null as character varying(255)) as "sig_v2_unit" ,
                    cast(null as numeric(10,3)) as "sig_v2_conf_score" ,
                    cast(null as character varying(255)) as "sig_v2_dosages" ,
                    cast(null as character varying(255)) as "sig_v2_scores" ,
                    cast(null as character varying(255)) as "sig_v2_frequencies" ,
                    cast(null as character varying(255)) as "sig_v2_durations" ,
                    cast(null as date) as "refill_date_next" ,
                    cast("refill_date_manual" as date) as "refill_date_manual" ,
                    cast("refill_date_default" as date) as "refill_date_default" ,
                    cast(null as numeric(11,3)) as "qty_total" ,
                    cast(null as character varying(80)) as "rx_source" ,
                    cast(null as character varying(80)) as "rx_transfer" ,
                    cast("groups" as character varying(255)) as "groups" ,
                    cast("rx_dispensed_id" as integer) as "rx_dispensed_id" ,
                    cast("stock_level_initial" as character varying(80)) as "stock_level_initial" ,
                    cast("rx_message_keys_initial" as character varying(255)) as "rx_message_keys_initial" ,
                    cast("patient_autofill_initial" as integer) as "patient_autofill_initial" ,
                    cast("rx_autofill_initial" as integer) as "rx_autofill_initial" ,
                    cast("rx_numbers_initial" as character varying(255)) as "rx_numbers_initial" ,
                    cast("zscore_initial" as numeric(6,3)) as "zscore_initial" ,
                    cast("refills_dispensed_default" as numeric(5,2)) as "refills_dispensed_default" ,
                    cast("refills_dispensed_actual" as numeric(5,2)) as "refills_dispensed_actual" ,
                    cast("days_dispensed_default" as integer) as "days_dispensed_default" ,
                    cast("days_dispensed_actual" as integer) as "days_dispensed_actual" ,
                    cast("qty_dispensed_default" as numeric(10,3)) as "qty_dispensed_default" ,
                    cast("qty_dispensed_actual" as numeric(10,3)) as "qty_dispensed_actual" ,
                    cast("price_dispensed_default" as numeric(5,2)) as "price_dispensed_default" ,
                    cast("price_dispensed_actual" as numeric(5,2)) as "price_dispensed_actual" ,
                    cast("qty_pended_total" as numeric(10,3)) as "qty_pended_total" ,
                    cast("qty_pended_repacks" as numeric(10,3)) as "qty_pended_repacks" ,
                    cast("count_pended_total" as integer) as "count_pended_total" ,
                    cast("count_pended_repacks" as integer) as "count_pended_repacks" ,
                    cast("item_message_keys" as character varying(255)) as "item_message_keys" ,
                    cast("item_message_text" as character varying(255)) as "item_message_text" ,
                    cast("item_type" as character varying(80)) as "item_type" ,
                    cast("item_added_by" as character varying(80)) as "item_added_by" ,
                    cast("item_date_added" as timestamp without time zone) as "item_date_added" ,
                    cast("refill_target_date" as timestamp without time zone) as "refill_target_date" ,
                    cast("refill_target_days" as integer) as "refill_target_days" ,
                    cast("refill_target_rxs" as character varying(255)) as "refill_target_rxs" 

            from "datawarehouse".analytics."order_items_historic"
        )

        
)
select
	*
from gph

	where _airbyte_emitted_at > (select MAX(_airbyte_emitted_at) from "datawarehouse".analytics."goodpill_historic")
