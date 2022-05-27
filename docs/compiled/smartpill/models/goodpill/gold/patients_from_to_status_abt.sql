with t1 as (
	select
		date_day
		, event_name_day as to_status
		, lag(event_name_day) over (partition by patient_id_cp order by date_day) as from_status
	from "datawarehouse".dev_analytics."patients_calendar_abt"
), t2 as (
    select
        count(*) as number_patients
        , date_day
        , to_status
        , from_status
    from t1
    where from_status is not null and from_status <> to_status
    group by date_day, to_status, from_status
)
select * from t2