with calc_statuses as (
	with all_dates as (
		select distinct
			patient_id_cp,
			has_refills,
			rx_date_expired,
			order_date_added,
			order_date_shipped,
			refill_date_next,
			refill_date_first,
			coalesce(lag(order_date_added, -1) over (partition by patient_id_cp order by order_date_added), now()) as next_row_order_date_added,
			patient_date_changed,
			patient_inactive
		from (
			select distinct on (rh.patient_id_cp, rh.rx_number, rh.refill_date_next, rh.updated_at, oh.order_date_added, patient_date_changed)
				rh.patient_id_cp,
				oh.order_date_added as order_date_added,
				oh.order_date_shipped as order_date_shipped,
				rh.refill_date_next as refill_date_next,
				rh.refills_left > 0 or rh.refills_total > 0 as has_refills,
				rh.rx_date_expired,
				rh.refill_date_first,
				p.patient_date_changed,
				p.patient_inactive is not null as patient_inactive
			from "datawarehouse".dev_analytics."rxs_joined" rh
			inner join "datawarehouse".dev_analytics."patients" p using (patient_id_cp)
			left join "datawarehouse".dev_analytics."order_items" oi using (rx_number, patient_id_cp)
			left join "datawarehouse".dev_analytics."orders" oh using (invoice_number, patient_id_cp)
			order by rh.patient_id_cp, rh.rx_number, rh.refill_date_next, rh.updated_at, oh.order_date_added, patient_date_changed
		) t
	)
	select distinct
		patient_id_cp,
		coalesce(order_date_added, refill_date_first) as event_date,
		'PATIENT_ACTIVE' as event_name
	from all_dates
	where order_date_added is not null or has_refills
	union
	select distinct
		patient_id_cp,
		coalesce(refill_date_next + interval '1' day, order_date_added + interval '4' month) as event_date,
		'PATIENT_CHURNED_OTHER' as event_name
	from all_dates
	where
		coalesce(refill_date_next + interval '1' day, order_date_added + interval '4' month) < next_row_order_date_added
		and order_date_shipped < next_row_order_date_added
		and has_refills and rx_date_expired <= coalesce(refill_date_next, order_date_added + interval '4' month)
		and (not patient_inactive or coalesce(refill_date_next, order_date_added + interval '4' month) < patient_date_changed)
	union
	select distinct
		patient_id_cp,
		coalesce(refill_date_next + interval '1' day, order_date_added + interval '4' month) as event_date,
		'PATIENT_CHURNED_NO_FILLABLE_RX' as event_name
	from all_dates
	where
		coalesce(refill_date_next + interval '1' day, order_date_added + interval '4' month) < next_row_order_date_added
		and order_date_shipped < next_row_order_date_added
		and not (has_refills or rx_date_expired <= coalesce(refill_date_next, order_date_added + interval '4' month))
		and (not patient_inactive or coalesce(refill_date_next, order_date_added + interval '4' month) < patient_date_changed)
),
statuses as (
	select
		gp.patient_id_cp,
		patient_date_added as event_date,
		'PATIENT_UNREGISTERED' as event_name
		from "datawarehouse".dev_analytics."patients" gp
		inner join (
			select distinct patient_id_cp from "datawarehouse".dev_analytics."rxs_joined" rxs
		) t using (patient_id_cp)
		where patient_date_added is not null
			and (patient_date_registered is null or date(patient_date_registered) >= date(patient_date_added))
	union
	select
		gp.patient_id_cp,
		patient_date_added as event_date,
		'PATIENT_NO_RX' as event_name
		from "datawarehouse".dev_analytics."patients" gp
		left join (
			select
				patient_id_cp,
				min(rx_date_added) as min_rx_date_added
			from "datawarehouse".dev_analytics."rxs_joined" rxs
			group by patient_id_cp
		) r on r.patient_id_cp = gp.patient_id_cp
		where patient_date_registered is not null
			and (r.patient_id_cp is null or patient_date_added < min_rx_date_added)
	union
	select
		patient_id_cp,
		patient_date_updated as event_date,
		'PATIENT_INACTIVE' as event_name
		from "datawarehouse".dev_analytics."patients"
		where patient_inactive = 'Inactive'
	union
	select
		patient_id_cp,
		patient_date_updated as event_date,
		'PATIENT_DECEASED' as event_name
		from "datawarehouse".dev_analytics."patients"
		where patient_inactive = 'Deceased'
)

select
	patient_id_cp,
	event_name,
	event_date,
	md5(cast(concat(event_name, patient_id_cp, event_date) as 
    varchar
)) as unique_event_id
from (
	select *
	from calc_statuses
	union
	select *
	from statuses
) gps

	where event_date > (select MAX(event_date) from "datawarehouse".dev_analytics."patients_status_historic")

order by patient_id_cp, event_name, event_date desc