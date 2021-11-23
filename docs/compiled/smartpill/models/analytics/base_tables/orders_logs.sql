select
    ol.invoice_number,
    patient_id_cp,
    order_zip,
    event_type,
    goodpill_event_date,
    count_items, 
    count_filled, 
    count_nofill,
    order_source, 
    order_stage_cp, 
    order_status,
    invoice_doc_id, 
    tracking_number, 
    order_date_changed, 
    order_date_updated,
    payment_total_default,
    payment_total_actual,
    payment_fee_default,
    payment_fee_actual,
    payment_due_default,
    payment_due_actual,
    payment_date_autopay,
    payment_method_actual,
    coupon_lines,
    order_note,
    date_processed
from analytics_v2.goodpill_gp_orders o
inner join analytics.`orders_logs_audit` ol on ol.invoice_number = o.invoice_number

    where date_processed > (select max(date_processed) from analytics.`orders_logs`)
