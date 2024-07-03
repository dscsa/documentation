with patients as (
    select *
    from "datawarehouse".goodpill."patients"
),
contact as (
    select *, contact_gp_patient_id_cp__c as patient_id_cp
    from "datawarehouse".salesforce."patients_contact"
	where contact_gp_patient_id_cp__c is not null --filter dummy cases
	and contact_isdeleted = false
),
contact_dummies as (
    select *, contact_gp_patient_id_cp__c as patient_id_cp
    from "datawarehouse".salesforce."patients_contact"
	where contact_gp_patient_id_cp__c is null --match dummy cases
	and contact_isdeleted = false
),
patients_x_contacts as (
	select
	p.*
	, psf.contact_account_id
	, psf.contact_last_modified_date
	, psf.contact_id
	, psf.contact_recordtype_id
	, case when (
		lower(p.first_name) is distinct from lower(psf.contact_firstname)
or lower(p.first_name) is distinct from lower(psf.contact_first_name__c)

or lower(p.email) is distinct from lower(psf.contact_email)
or lower(p.email) is distinct from lower(psf.contact_gp_email__c)

or p.patient_id_wc is distinct from psf.contact_gp_patient_id_wc__c
or lower(p.language) is distinct from lower(psf.contact_gp_language__c)
or lower(p.patient_address1) is distinct from lower(psf.contact_gp_patient_address1__c)
or lower(p.patient_address2) is distinct from lower(psf.contact_gp_patient_address2__c)
or p.patient_autofill is distinct from psf.contact_gp_patient_autofill__c::float::int

or lower(p.patient_city) is distinct from lower(psf.contact_gp_patient_city__c)
-- 	date_added__c & date_changed__c may are updated because a related entity changed. So it
--	doesnt mean that the record itself changed (Adam)
--	or p.patient_date_added is distinct from psf.contact_gp_patient_date_added__c
--	or p.patient_date_changed is distinct from psf.contact_gp_patient_date_changed__c

-- Salesforce Next Line char is not equal to MariaDB Next Line char, so I replace all variations to be equal
-- https://stackoverflow.com/questions/7836906/how-to-remove-carriage-returns-and-new-lines-in-postgresql
-- https://en.wikipedia.org/wiki/Newline#Unicode
or trim(regexp_replace( substring(lower(p.patient_note),1,3000), E'[\\n\\r\\f\\u000B\\u0085\\u2028\\u2029]+', ' ', 'g' )) is distinct from trim(regexp_replace( substring(lower(psf.contact_gp_patient_note__c),1,3000), E'[\\n\\r\\f\\u000B\\u0085\\u2028\\u2029]+', ' ', 'g' ))

or lower(p.patient_state) is distinct from lower(psf.contact_gp_patient_state__c)
or lower(p.patient_inactive) is distinct from lower(psf.contact_gp_patient_status__c)
or lower(p.patient_zip) is distinct from lower(psf.contact_gp_patient_zip__c)

or p.payment_card_date_expired is distinct from psf.contact_gp_payment_card_date_expired__c
or lower(p.payment_card_last4) is distinct from lower(psf.contact_gp_payment_card_last4__c)
or lower(p.payment_card_type) is distinct from lower(psf.contact_gp_payment_card_type__c)

or lower(p.payment_coupon) is distinct from lower(psf.contact_gp_payment_coupon__c)
or lower(p.payment_method_default) is distinct from lower(psf.contact_gp_payment_method__c)

or lower(p.pharmacy_fax) is distinct from lower(psf.contact_gp_pharmacy_fax__c)
or lower(p.pharmacy_npi) is distinct from lower(psf.contact_gp_pharmacy_npi__c)
or lower(p.pharmacy_name) is distinct from lower(psf.contact_gp_pharmacy_name__c)
or lower(p.pharmacy_phone) is distinct from lower(psf.contact_gp_pharmacy_phone__c)
or lower(p.pharmacy_address) is distinct from lower(psf.contact_gp_pharmacy_address__c)

or p.phone1 is distinct from psf.contact_gp_phone1__c
or p.phone2 is distinct from psf.contact_gp_phone2__c

or p.refills_used is distinct from psf.contact_gp_refills_used__c::numeric(5,2)
or p.tracking_coupon is distinct from psf.contact_gp_tracking_coupon__c

or DATE(p.birth_date) is distinct from DATE(psf.contact_birthdate)
	) then 1 else 0 end as need_update

	from patients p
	inner join contact psf on (p.patient_id_cp = psf.patient_id_cp)
),
patients_x_dummy_contacts as (
	select
	p.*
	, psf.contact_account_id
	, psf.contact_last_modified_date
	, psf.contact_id
	, psf.contact_recordtype_id
	, case when (
		lower(p.first_name) is distinct from lower(psf.contact_firstname)
or lower(p.first_name) is distinct from lower(psf.contact_first_name__c)

or lower(p.email) is distinct from lower(psf.contact_email)
or lower(p.email) is distinct from lower(psf.contact_gp_email__c)

or p.patient_id_wc is distinct from psf.contact_gp_patient_id_wc__c
or lower(p.language) is distinct from lower(psf.contact_gp_language__c)
or lower(p.patient_address1) is distinct from lower(psf.contact_gp_patient_address1__c)
or lower(p.patient_address2) is distinct from lower(psf.contact_gp_patient_address2__c)
or p.patient_autofill is distinct from psf.contact_gp_patient_autofill__c::float::int

or lower(p.patient_city) is distinct from lower(psf.contact_gp_patient_city__c)
-- 	date_added__c & date_changed__c may are updated because a related entity changed. So it
--	doesnt mean that the record itself changed (Adam)
--	or p.patient_date_added is distinct from psf.contact_gp_patient_date_added__c
--	or p.patient_date_changed is distinct from psf.contact_gp_patient_date_changed__c

-- Salesforce Next Line char is not equal to MariaDB Next Line char, so I replace all variations to be equal
-- https://stackoverflow.com/questions/7836906/how-to-remove-carriage-returns-and-new-lines-in-postgresql
-- https://en.wikipedia.org/wiki/Newline#Unicode
or trim(regexp_replace( substring(lower(p.patient_note),1,3000), E'[\\n\\r\\f\\u000B\\u0085\\u2028\\u2029]+', ' ', 'g' )) is distinct from trim(regexp_replace( substring(lower(psf.contact_gp_patient_note__c),1,3000), E'[\\n\\r\\f\\u000B\\u0085\\u2028\\u2029]+', ' ', 'g' ))

or lower(p.patient_state) is distinct from lower(psf.contact_gp_patient_state__c)
or lower(p.patient_inactive) is distinct from lower(psf.contact_gp_patient_status__c)
or lower(p.patient_zip) is distinct from lower(psf.contact_gp_patient_zip__c)

or p.payment_card_date_expired is distinct from psf.contact_gp_payment_card_date_expired__c
or lower(p.payment_card_last4) is distinct from lower(psf.contact_gp_payment_card_last4__c)
or lower(p.payment_card_type) is distinct from lower(psf.contact_gp_payment_card_type__c)

or lower(p.payment_coupon) is distinct from lower(psf.contact_gp_payment_coupon__c)
or lower(p.payment_method_default) is distinct from lower(psf.contact_gp_payment_method__c)

or lower(p.pharmacy_fax) is distinct from lower(psf.contact_gp_pharmacy_fax__c)
or lower(p.pharmacy_npi) is distinct from lower(psf.contact_gp_pharmacy_npi__c)
or lower(p.pharmacy_name) is distinct from lower(psf.contact_gp_pharmacy_name__c)
or lower(p.pharmacy_phone) is distinct from lower(psf.contact_gp_pharmacy_phone__c)
or lower(p.pharmacy_address) is distinct from lower(psf.contact_gp_pharmacy_address__c)

or p.phone1 is distinct from psf.contact_gp_phone1__c
or p.phone2 is distinct from psf.contact_gp_phone2__c

or p.refills_used is distinct from psf.contact_gp_refills_used__c::numeric(5,2)
or p.tracking_coupon is distinct from psf.contact_gp_tracking_coupon__c

or DATE(p.birth_date) is distinct from DATE(psf.contact_birthdate)
	) then 1 else 0 end as need_update
	
	from patients p
	inner join contact_dummies psf on (
		(trim(lower(p.first_name || ' ' || p.last_name || ' ' || date(p.birth_date))) = trim(lower(psf.contact_name))) 
	)
),
join_patients as (
	select * from patients_x_contacts
	union all
	select * from patients_x_dummy_contacts
)
select
	contact_id as "Id",
	patient_id_cp as gp_patient_id_cp__c,
	patient_id_wc as gp_patient_id_wc__c,
	contact_account_id as "AccountId",
	contact_recordtype_id as "RecordTypeId",

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

from join_patients
where need_update = 1
order by contact_last_modified_date desc