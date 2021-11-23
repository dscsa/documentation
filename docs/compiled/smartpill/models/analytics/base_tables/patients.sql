select
    patient_id_cp,
    patient_date_registered,
    patient_date_added,
    @fill_next := (
        select MAX(grg.refill_date_next)
        from analytics_v2.goodpill_gp_rxs_grouped grg
        where grg.patient_id_cp = gpa.patient_id_cp
    ) as fill_next,
    DATEDIFF(
        NOW(),
        STR_TO_DATE(@fill_next, '%Y-%m-%dT%H:%i:%sZ')
    ) as days_overdue,
    first_name,
    last_name,
    birth_date,
    phone1,
    phone2,
    CONCAT(patient_address1, ', ', patient_address2),
    patient_city,
    patient_state,
    patient_zip,
    payment_card_type,
    payment_card_last4,
    payment_card_date_expired,
    payment_method_default,
    payment_coupon,
    tracking_coupon,
    refills_used,
    NOW() as date_processed
from analytics_v2.goodpill_gp_patients gpa
where
    LOWER(first_name) NOT LIKE '%test%' AND
    LOWER(first_name) NOT LIKE '%user%' AND
    LOWER(last_name) NOT LIKE '%test%' AND
    LOWER(last_name) NOT LIKE '%user%'

    AND _airbyte_emitted_at > (SELECT MAX(date_processed) FROM analytics.`patients`)
