-- Exclude the common columns between the tables,
--  to be called with dbt_utils.star (instead of using
--  the * operator to select all columns).



select
    max(
      case
      when cph.event_name = 'CLINIC_META_ADDED'
        then cph.event_date
      else null
      end
    ) over(partition by (coupon_code, clinic_regular_name, npi_number, provider_name)) as date_clinic_meta_added,
    max(
      case
      when cph.event_name = 'CLINIC_META_UPDATED'
        then cph.event_date
      else null
      end
    ) over(partition by (coupon_code, clinic_regular_name, npi_number, provider_name)) as date_clinic_meta_updated,
	max(
      case
      when cph.event_name = 'CLINIC_META_DELETED'
        then cph.event_date
      else null
      end
    ) over(partition by (coupon_code, clinic_regular_name, npi_number, provider_name)) as date_clinic_meta_deleted,
	cph."coupon_code" as "clinic_coupon_code",
	cph."clinic_regular_name" as "clinic_regular_name",
	cph."npi_number" as "clinic_npi_number",
	cph."provider_name" as "clinic_provider_name",
	cph."clinic_name" as "clinic_name",
	cph."updated_at" as "clinic_updated_at",
	cph."event_date" as "clinic_event_date"
from "datawarehouse".dev_analytics."clinics_providers_historic" cph