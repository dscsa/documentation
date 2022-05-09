-- Exclude the common columns between the tables,
--  to be called with dbt_utils.star (instead of using
--  the * operator to select all columns).



select
	rh.patient_id_cp,
	event_date as rx_event_date,
	rh.rx_number,
	first_value(provider_npi) over (
		partition by rx_number
		order by case when provider_npi is not null then 0 else 1 end, event_date desc nulls last
	) as rx_provider_npi,
	first_value(drug_generic) over (
		partition by rx_number
		order by case when drug_generic is not null then 0 else 1 end, event_date desc nulls last
	) as rx_drug_generic,
    max(  
      case
      when rh.event_name = 'RX_UPDATED'
        then rh.event_date
      else null
      end
    ) over(partition by rx_number) as rx_date_updated,
  
    max(
      case
      when rh.event_name = 'RX_WRITTEN'
        then rh.event_date
      else null
      end
    ) over(partition by rx_number) as rx_date_written,
  
    max(
      case
      when rh.event_name = 'RX_TRANSFERRED'
        then rh.event_date
      else null
      end
    ) over(partition by rx_number) as date_rx_transferred,
  
    max(
      case
      when rh.event_name = 'RX_ADDED'
        then rh.event_date
      else null
      end
    ) over(partition by rx_number) as rx_date_added ,
	rh."clinic_name" as "rx_clinic_name",
	rh."is_refill" as "rx_is_refill",
	rh."rx_autofill" as "rx_autofill",
	rh."sig_qty_per_day" as "rx_sig_qty_per_day",
	rh."rx_message_key" as "rx_message_key",
	rh."max_gsn" as "rx_max_gsn",
	rh."drug_gsns" as "rx_drug_gsns",
	rh."refills_total" as "rx_refills_total",
	rh."refills_original" as "rx_refills_original",
	rh."refills_left" as "rx_refills_left",
	rh."refill_date_first" as "rx_refill_date_first",
	rh."refill_date_last" as "rx_refill_date_last",
	rh."rx_date_expired" as "rx_date_expired",
	rh."rx_date_changed" as "rx_date_changed",
	rh."qty_left" as "rx_qty_left",
	rh."qty_original" as "rx_qty_original",
	rh."sig_actual" as "rx_sig_actual",
	rh."sig_initial" as "rx_sig_initial",
	rh."sig_clean" as "rx_sig_clean",
	rh."sig_qty" as "rx_sig_qty",
	rh."sig_days" as "rx_sig_days",
	rh."sig_qty_per_day_actual" as "rx_sig_qty_per_day_actual",
	rh."sig_v2_qty" as "rx_sig_v2_qty",
	rh."sig_v2_days" as "rx_sig_v2_days",
	rh."sig_v2_qty_per_day" as "rx_sig_v2_qty_per_day",
	rh."sig_v2_unit" as "rx_sig_v2_unit",
	rh."sig_v2_conf_score" as "rx_sig_v2_conf_score",
	rh."sig_v2_dosages" as "rx_sig_v2_dosages",
	rh."sig_v2_scores" as "rx_sig_v2_scores",
	rh."sig_v2_frequencies" as "rx_sig_v2_frequencies",
	rh."sig_v2_durations" as "rx_sig_v2_durations",
	rh."refill_date_next" as "rx_refill_date_next",
	rh."refill_date_manual" as "rx_refill_date_manual",
	rh."refill_date_default" as "rx_refill_date_default",
	rh."qty_total" as "rx_qty_total",
	rh."rx_source" as "rx_source",
	rh."rx_transfer" as "rx_transfer"

from "datawarehouse".dev_analytics."rxs_historic" rh