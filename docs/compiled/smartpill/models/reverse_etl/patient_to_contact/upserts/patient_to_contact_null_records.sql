with patients as (
    select * from "datawarehouse".goodpill."patients"
    where patient_id_cp in (select gp_patient_id_cp__c from "datawarehouse".reverse_etl."patient_to_contact_editions")
),
contact as (
    select *, contact_gp_patient_id_cp__c as patient_id_cp
    from "datawarehouse".salesforce."patients_contact"
	where contact_gp_patient_id_cp__c is not null --filter dummy cases
	and contact_isdeleted = false
),
patients_null_cases as (
	select p.*, psf.* from patients p
	inner join contact psf on (p.patient_id_cp = psf.patient_id_cp)
    where
    (
        (email is null and contact_email is not null)
        or (email is null and contact_gp_email__c is not null)
        or (patient_address1 is null and contact_gp_patient_address1__c is not null)
        or (patient_address2 is null and contact_gp_patient_address2__c is not null)
        or (payment_card_date_expired is null and contact_gp_payment_card_date_expired__c is not null)
        or (payment_card_last4 is null and contact_gp_payment_card_last4__c is not null)
        or (payment_card_type is null and contact_gp_payment_card_type__c is not null)
        or (payment_coupon is null and contact_gp_payment_coupon__c is not null)
        or (pharmacy_fax is null and contact_gp_pharmacy_fax__c is not null)
        or (pharmacy_npi is null and contact_gp_pharmacy_npi__c is not null)
        or (pharmacy_name is null and contact_gp_pharmacy_name__c is not null)
        or (pharmacy_phone is null and contact_gp_pharmacy_phone__c is not null)
        or (pharmacy_address is null and contact_gp_pharmacy_address__c is not null)
        or (phone1 is null and contact_gp_phone1__c is not null)
        or (phone2 is null and contact_gp_phone2__c is not null)
        or (language is null and contact_gp_language__c is not null)
        or (patient_city is null and contact_gp_patient_city__c is not null)
        or (patient_state is null and contact_gp_patient_state__c is not null)
        or (patient_zip is null and contact_gp_patient_zip__c is not null)
        or (payment_method_default is null and contact_gp_payment_method__c is not null)
        or (tracking_coupon is null and contact_gp_tracking_coupon__c is not null)
    )
    AND (
        p.patient_id_cp not in (
            select patient_id_cp
            from "datawarehouse"."reverse_etl"."patient_to_contact_non_updated"
            where its_fixed = false
            and patient_id_cp is not null
        )
        and contact_id not in (
            select salesforce_contact_id
            from "datawarehouse"."reverse_etl"."patient_to_contact_non_updated"
            where its_fixed = false
            and salesforce_contact_id is not null
        )
        and (
            lower(contact_lastname) not like '%test%'
            and lower(contact_name) not like '%test%'
            and lower(contact_name) not like '%fake%'
            and lower(contact_name) not like '%user%'
            and lower(contact_email) not like '%test%'
        )
    )
)
select
    contact_id as "WhoId"
    , 'Patient information removed from Webform' as "Subject"
    , 'a001M00000aT1ZoQAK' as "WhatId" -- ".Missing Contact Info"
    , 'a001M00000aT1ZoQAK' as "Assigned_To__c" -- ".Missing Contact Info"
    , to_char(now(), 'YYYY-MM-dd') as "ActivityDate"
    , CONCAT(
        'Patient had additional information in Salesforce that was NOT in Webform. Please review the additional information and determine if it needs to be added to Webform. You may need to call the patient to verify. Related fields: ', E'\n',
        case when ((email is null and contact_email is not null) or (email is null and contact_gp_email__c is not null)) then '- email: ' || contact_gp_email__c || E'\n' end,
        case when (patient_address1 is null and contact_gp_patient_address1__c is not null) then '- patient_address1: ' || contact_gp_patient_address1__c || E'\n' end,
        case when (patient_address2 is null and contact_gp_patient_address2__c is not null) then '- patient_address2: ' || contact_gp_patient_address2__c || E'\n' end,
        case when (payment_card_date_expired is null and contact_gp_payment_card_date_expired__c is not null) then '- payment_card_date_expired: ' || contact_gp_payment_card_date_expired__c || E'\n' end,
        case when (payment_card_last4 is null and contact_gp_payment_card_last4__c is not null) then '- payment_card_last4: ' || contact_gp_payment_card_last4__c || E'\n' end,
        case when (payment_card_type is null and contact_gp_payment_card_type__c is not null) then '- payment_card_type: ' || contact_gp_payment_card_type__c || E'\n' end,
        case when (payment_coupon is null and contact_gp_payment_coupon__c is not null) then '- payment_coupon: ' || contact_gp_payment_coupon__c || E'\n' end,
        case when (pharmacy_fax is null and contact_gp_pharmacy_fax__c is not null) then '- pharmacy_fax: ' || contact_gp_pharmacy_fax__c || E'\n' end,
        case when (pharmacy_npi is null and contact_gp_pharmacy_npi__c is not null) then '- pharmacy_npi: ' || contact_gp_pharmacy_npi__c || E'\n' end,
        case when (pharmacy_name is null and contact_gp_pharmacy_name__c is not null) then '- pharmacy_name: ' || contact_gp_pharmacy_name__c || E'\n' end,
        case when (pharmacy_phone is null and contact_gp_pharmacy_phone__c is not null) then '- pharmacy_phone: ' || contact_gp_pharmacy_phone__c || E'\n' end,
        case when (pharmacy_address is null and contact_gp_pharmacy_address__c is not null) then '- pharmacy_address: ' || contact_gp_pharmacy_address__c || E'\n' end,
        case when (phone1 is null and contact_gp_phone1__c is not null) then '- phone1: ' || contact_gp_phone1__c || E'\n' end,
        case when (phone2 is null and contact_gp_phone2__c is not null) then '- phone2: ' || contact_gp_phone2__c || E'\n' end,
        case when (language is null and contact_gp_language__c is not null) then '- language: ' || contact_gp_language__c || E'\n' end,
        case when (patient_city is null and contact_gp_patient_city__c is not null) then '- patient_city: ' || contact_gp_patient_city__c || E'\n' end,
        case when (patient_state is null and contact_gp_patient_state__c is not null) then '- patient_state: ' || contact_gp_patient_state__c || E'\n' end,
        case when (patient_zip is null and contact_gp_patient_zip__c is not null) then '- patient_zip: ' || contact_gp_patient_zip__c || E'\n' end,
        case when (payment_method_default is null and contact_gp_payment_method__c is not null) then '- payment_method_default: ' || contact_gp_payment_method__c || E'\n' end,
        case when (tracking_coupon is null and contact_gp_tracking_coupon__c is not null) then '- tracking_coupon: ' || contact_gp_tracking_coupon__c || E'\n' end
    ) as "Description"
from patients_null_cases