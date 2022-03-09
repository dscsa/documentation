

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
    
    

    )

    select *
    from unioned
    where generated_number <= 1893
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
		from "datawarehouse".prod_analytics."patients"
	)

	-- assign to all the rows of a partition its designated event
	select
		patient_id_cp,
		date_day,
		first_value(event_weight) over (partition by patient_id_cp, partition_day order by date_day) as event_weight_day
	from (
		-- cross join dates and patient ids, left join historic statuses.
		-- mark "partitions" for all dates corresponding to a single event
		select
			pids.patient_id_cp,
			calendar.date_day,
			pe.event_weight,
			sum(case when psh.event_name is null then 0 else 1 end) over (partition by pids.patient_id_cp order by calendar.date_day, event_weight asc) as partition_day
		from calendar
		cross join pids
		left join "datawarehouse".prod_analytics."patients_status_historic" psh on date(psh.event_date) = calendar.date_day and psh.patient_id_cp = pids.patient_id_cp
		left join "datawarehouse".prod_analytics."patient_events" pe on pe.event_name = psh.event_name
	) t
), pci as (
	select
		patient_id_cp,
		date_day,
		case
			when pe.event_name = 'PATIENT_ACTIVE' and lag(pe.event_name, 1) over (partition by pc.patient_id_cp) in ('PATIENT_UNREGISTED', 'PATIENT_NO_RX') then (select event_weight from "datawarehouse".prod_analytics."patient_events" where event_name = 'PATIENT_NEW_ACTIVE')
			when pe.event_name like '%CHURN%' and lag(pe.event_name, 1) over (partition by pc.patient_id_cp) = 'PATIENT_ACTIVE' then (select event_weight from "datawarehouse".prod_analytics."patient_events" where event_name = 'PATIENT_NEW_CHURN')
			when pe.event_name like 'PATIENT_ACTIVE' and lag(pe.event_name, 1) over (partition by pc.patient_id_cp) like '%CHURN%' then (select event_weight from "datawarehouse".prod_analytics."patient_events" where event_name = 'PATIENT_REACTIVATED')
			else pc.event_weight_day
		end as event_weight_day
	from pc
	left join "datawarehouse".prod_analytics."patient_events" pe on pc.event_weight_day = pe.event_weight
)

-- obtain the event date for each time frame
select distinct on (patient_id_cp, date_day)
	patient_id_cp,
	date_day,
	max(event_weight_day) over (partition by patient_id_cp, date_day) as event_weight_day,
	max(event_weight_day) over (partition by patient_id_cp, extract(year from date_day), extract(week from date_day)) as event_weight_week,
	max(event_weight_day) over (partition by patient_id_cp, extract(year from date_day), extract(month from date_day)) as event_weight_month,
	max(event_weight_day) over (partition by patient_id_cp, extract(year from date_day)) as event_weight_year
from pci