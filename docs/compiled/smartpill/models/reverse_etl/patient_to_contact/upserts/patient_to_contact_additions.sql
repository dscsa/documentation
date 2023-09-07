with patients as (
    select *
	, (trim(lower(p.first_name || ' ' || p.last_name || ' ' || date(p.birth_date)))) as contact_name_trim
    from "datawarehouse".goodpill."patients" p
),
contact as (
    select 
	contact_id
	, contact_gp_patient_id_cp__c as patient_id_cp
	, trim(lower(psf.contact_name)) as contact_name_trim
    from "datawarehouse".salesforce."patients_contact" psf
	where contact_isdeleted = false
),
patients_x_contact as (
	-- first we match by the ones that didnt join using id cp
	select p.*, psf.contact_id
	from patients p 
	left join contact psf on (p.patient_id_cp = psf.patient_id_cp)
	where (psf.patient_id_cp is null) -- is new patient
),
patients_x_contact_final as (
	-- then from the remaining ones (just a few) we match by name+DOB
	select p.* from patients_x_contact p
	left join contact psf on (p.contact_name_trim = psf.contact_name_trim)
	where (psf.contact_name_trim is null) -- is new patient
)
select
	contact_id as "Id", -- is null because it didnt join
	patient_id_cp as gp_patient_id_cp__c,
	patient_id_wc as gp_patient_id_wc__c,
	'0011M00002Mnf3QQAR' as "AccountId", -- GoodPill Home Delivery Id
	'0121M000001BQ4dQAG' as "RecordTypeId", -- Goodpill Layout Id

	last_name || ' ' || date(birth_date) as "LastName",
	date(birth_date) as "Birthdate",

	first_name as first_name__c,
	first_name as "FirstName",

	email as gp_email__c,
	email as "Email",

	language as gp_language__c,
	patient_address1 as gp_patient_address1__c,
	patient_address2 as gp_patient_address2__c,
	patient_autofill as gp_patient_autofill__c,

	patient_city as gp_patient_city__c,
	to_char(patient_date_added, 'YYYY-MM-DDThh24:mi:ss.0000Z') as gp_patient_date_added__c,
	to_char(patient_date_changed, 'YYYY-MM-DDThh24:mi:ss.0000Z') as gp_patient_date_changed__c,

	regexp_replace( substring(lower(patient_note),1,3000), E'[\\n\\r\\f\\u000B\\u0085\\u2028\\u2029]+', ' ', 'g' ) as gp_patient_note__c,

	patient_state as gp_patient_state__c,
	patient_inactive as gp_patient_status__c,
	patient_zip as gp_patient_zip__c,

	to_char(payment_card_date_expired, 'YYYY-MM-DDThh24:mi:ss.0000Z') as gp_payment_card_date_expired__c,
	payment_card_last4 as gp_payment_card_last4__c,
	payment_card_type as gp_payment_card_type__c,

	payment_coupon as gp_payment_coupon__c,
	payment_method_default as gp_payment_method__c,

	pharmacy_fax as gp_pharmacy_fax__c,
	pharmacy_npi as gp_pharmacy_npi__c,
	pharmacy_name as gp_pharmacy_name__c,
	pharmacy_phone as gp_pharmacy_phone__c,
	pharmacy_address as gp_pharmacy_address__c,

	phone1 as gp_phone1__c,
	phone2 as gp_phone2__c,

	refills_used as gp_refills_used__c,
	tracking_coupon as gp_tracking_coupon__c

from patients_x_contact_final