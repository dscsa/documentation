select
    invoice_number,
    event_type,
    goodpill_event_date,
    patient_id,
    _airbyte_emitted_at as date_processed
from
    analytics_v2.analytics_orders_logs ol


    where _airbyte_emitted_at > (select max(_airbyte_emitted_at) from analytics.`orders_logs_audit`)
