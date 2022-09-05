with patients_calendar as (
    with pc as (
        -- table with a range of days
        with calendar as (
            

/*
call as follows:

date_spine(
    "day",
    "to_date('01/01/2016', 'mm/dd/yyyy')",
    "dateadd(week, 1, current_date)"
)

*/

with rawdata as (

    

    

    with p as (
        select 0 as generated_number union all select 1
    ), unioned as (

    select

    
    p0.generated_number * power(2, 0)
     + 
    
    p1.generated_number * power(2, 1)
     + 
    
    p2.generated_number * power(2, 2)
     + 
    
    p3.generated_number * power(2, 3)
     + 
    
    p4.generated_number * power(2, 4)
     + 
    
    p5.generated_number * power(2, 5)
     + 
    
    p6.generated_number * power(2, 6)
     + 
    
    p7.generated_number * power(2, 7)
     + 
    
    p8.generated_number * power(2, 8)
     + 
    
    p9.generated_number * power(2, 9)
     + 
    
    p10.generated_number * power(2, 10)
     + 
    
    p11.generated_number * power(2, 11)
    
    
    + 1
    as generated_number

    from

    
    p as p0
     cross join 
    
    p as p1
     cross join 
    
    p as p2
     cross join 
    
    p as p3
     cross join 
    
    p as p4
     cross join 
    
    p as p5
     cross join 
    
    p as p6
     cross join 
    
    p as p7
     cross join 
    
    p as p8
     cross join 
    
    p as p9
     cross join 
    
    p as p10
     cross join 
    
    p as p11
    
    

    )

    select *
    from unioned
    where generated_number <= 2073
    order by generated_number



),

all_periods as (

    select (
        

    cast('2017-01-01' as date) + ((interval '1 day') * (row_number() over (order by 1) - 1))


    ) as date_day
    from rawdata

),

filtered as (

    select *
    from all_periods
    where date_day <= cast(now() as date)

)

select * from filtered


        ),

        -- table with all distincts patient ids, to cross join with dates
        pids as (
            select distinct patient_id_cp
            from "datawarehouse".dev_analytics."patients"
        )

        -- assign to all the rows of a partition its designated event
        select
            patient_id_cp,
            date_day,
            first_value(
                event_weight
            ) over (partition by patient_id_cp, partition_day order by date_day) as event_weight_day
        from (
            -- cross join dates and patient ids, left join historic statuses.
            -- mark "partitions" for all dates corresponding to a single event
            select
                pids.patient_id_cp,
                calendar.date_day,
                pe.event_weight,
                sum(
                    case when psh.event_name is null then 0 else 1 end
                ) over (
                    partition by pids.patient_id_cp order by calendar.date_day, pe.event_weight asc
                ) as partition_day
            from calendar
            cross join pids
            left join
                "datawarehouse".dev_analytics."patients_status_historic" as psh on
                    date(psh.event_date) = calendar.date_day and psh.patient_id_cp = pids.patient_id_cp
            left join "datawarehouse".dev_analytics."patient_events" as pe on pe.event_name = psh.event_name
        ) as t
    ),

    pci as (
        select
            patient_id_cp,
            date_day,
            case
                when
                    pe.event_name = 'PATIENT_ACTIVE' and lag(
                        pe.event_name, 1
                    ) over (
                        partition by pc.patient_id_cp
                    ) in (
                        'PATIENT_UNREGISTERED', 'PATIENT_NO_RX'
                    ) then (
                        select event_weight from "datawarehouse".dev_analytics."patient_events" where event_name = 'PATIENT_NEW_ACTIVE'
                    )
                when
                    pe.event_name like '%CHURN%' and lag(
                        pe.event_name, 1
                    ) over (
                        partition by pc.patient_id_cp
                    ) = 'PATIENT_ACTIVE' then (
                        select event_weight from "datawarehouse".dev_analytics."patient_events" where event_name = 'PATIENT_NEW_CHURN'
                    )
                when
                    pe.event_name like 'PATIENT_ACTIVE' and lag(
                        pe.event_name, 1
                    ) over (
                        partition by pc.patient_id_cp
                    ) like '%CHURN%' then (
                        select event_weight from "datawarehouse".dev_analytics."patient_events" where event_name = 'PATIENT_REACTIVATED'
                    )
                else pc.event_weight_day
            end as event_weight_day
        from pc
        left join "datawarehouse".dev_analytics."patient_events" as pe on pc.event_weight_day = pe.event_weight
    )

    -- obtain the event date for each time frame
    select distinct on (patient_id_cp, date_day)
        patient_id_cp,
        date_day,
        max(event_weight_day) over (partition by patient_id_cp, date_day) as event_weight_day,
        max(
            event_weight_day
        ) over (
            partition by patient_id_cp, extract(year from date_day), extract(week from date_day)
        ) as event_weight_week,
        max(
            event_weight_day
        ) over (
            partition by patient_id_cp, extract(year from date_day), extract(month from date_day)
        ) as event_weight_month,
        max(event_weight_day) over (partition by patient_id_cp, extract(year from date_day)) as event_weight_year
    from pci

)


select
    patients_calendar.patient_id_cp,
    patients_calendar.date_day,
    ped.event_name as event_name_day,
    pew.event_name as event_name_week,
    pem.event_name as event_name_month,
    pey.event_name as event_name_year,
    p."patient_date_registered" as "patient_date_registered",
  p."patient_date_added" as "patient_date_added",
  p."patient_date_changed" as "patient_date_changed",
  p."first_name" as "patient_first_name",
  p."last_name" as "patient_last_name",
  p."birth_date" as "patient_birth_date",
  p."language" as "patient_language",
  p."phone1" as "patient_phone1",
  p."phone2" as "patient_phone2",
  p."patient_address" as "patient_address",
  p."patient_city" as "patient_city",
  p."patient_state" as "patient_state",
  p."patient_zip" as "patient_zip",
  p."payment_card_type" as "patient_payment_card_type",
  p."payment_card_last4" as "patient_payment_card_last4",
  p."payment_card_date_expired" as "patient_payment_card_date_expired",
  p."payment_card_autopay" as "patient_payment_card_autopay",
  p."payment_method_default" as "patient_payment_method_default",
  p."clinic_id_coupon" as "patient_clinic_id_coupon",
  p."payment_coupon" as "patient_payment_coupon",
  p."tracking_coupon" as "patient_tracking_coupon",
  p."patient_date_first_rx_received" as "patient_date_first_rx_received",
  p."patient_date_first_dispensed" as "patient_date_first_dispensed",
  p."patient_date_first_expected_by" as "patient_date_first_expected_by",
  p."refills_used" as "patient_refills_used",
  p."pharmacy_npi" as "patient_pharmacy_npi",
  p."pharmacy_name" as "patient_pharmacy_name",
  p."pharmacy_phone" as "patient_pharmacy_phone",
  p."pharmacy_fax" as "patient_pharmacy_fax",
  p."pharmacy_address" as "patient_pharmacy_address",
  p."patient_inactive" as "patient_inactive",
  p."patient_id_wc" as "patient_id_wc",
  p."email" as "patient_email",
  p."patient_autofill" as "patient_autofill",
  p."patient_note" as "patient_note",
  p."initial_invoice_number" as "patient_initial_invoice_number",
  p."allergies_none" as "patient_allergies_none",
  p."allergies_cephalosporins" as "patient_allergies_cephalosporins",
  p."allergies_sulfa" as "patient_allergies_sulfa",
  p."allergies_aspirin" as "patient_allergies_aspirin",
  p."allergies_penicillin" as "patient_allergies_penicillin",
  p."allergies_erythromycin" as "patient_allergies_erythromycin",
  p."allergies_codeine" as "patient_allergies_codeine",
  p."allergies_nsaids" as "patient_allergies_nsaids",
  p."allergies_salicylates" as "patient_allergies_salicylates",
  p."allergies_azithromycin" as "patient_allergies_azithromycin",
  p."allergies_amoxicillin" as "patient_allergies_amoxicillin",
  p."allergies_tetracycline" as "patient_allergies_tetracycline",
  p."allergies_other" as "patient_allergies_other",
  p."medications_other" as "patient_medications_other",
  p."patient_date_updated" as "patient_date_updated",
  p."date_processed" as "patient_date_processed"
from patients_calendar
inner join "datawarehouse".dev_analytics."patients" as p on patients_calendar.patient_id_cp = p.patient_id_cp
inner join "datawarehouse".dev_analytics."patient_events" as ped on patients_calendar.event_weight_day = ped.event_weight
inner join "datawarehouse".dev_analytics."patient_events" as pew on patients_calendar.event_weight_week = pew.event_weight
inner join "datawarehouse".dev_analytics."patient_events" as pem on patients_calendar.event_weight_month = pem.event_weight
inner join "datawarehouse".dev_analytics."patient_events" as pey on patients_calendar.event_weight_year = pey.event_weight