with calc_statuses as (
	with all_dates as (
		select
			patient_id_cp,
			has_refills,
			rx_date_expired,
			coalesce(order_date_added, refill_date_first) as order_date_added,
			coalesce(order_date_shipped, refill_date_first) as order_date_shipped,
			coalesce(refill_date_next, refill_date_first) as refill_date_next,
			refill_date_first,
			coalesce(lag(order_date_added, -1) over (partition by patient_id_cp order by order_date_added), now()) as next_row_order_date_added,
			patient_date_changed,
			patient_inactive
		from (
			select
				rh.patient_id_cp,
				coalesce(oh.order_date_added, rh.refill_date_first) as order_date_added,
				oh.order_date_shipped as order_date_shipped,
				rh.refill_date_next as refill_date_next,
				rh.refills_left > 0 or rh.refills_total > 0 as has_refills,
				rh.rx_date_expired,
				rh.refill_date_first,
				p.patient_date_changed,
				p.patient_inactive is not null as patient_inactive
			from "datawarehouse".goodpill."rxs_joined" rh
			inner join "datawarehouse".goodpill."patients" p using (patient_id_cp)
			left join "datawarehouse".goodpill."order_items" oi using (rx_number, patient_id_cp)
			left join "datawarehouse".goodpill."orders" oh using (invoice_number, patient_id_cp)
		) t
	)
	select distinct
		patient_id_cp,
		coalesce(order_date_added, refill_date_first) as event_date,
		'PATIENT_ACTIVE' as event_name
	from all_dates
	where order_date_added is not null or (has_refills and refill_date_first is not null)
	union
	select distinct
		patient_id_cp,
		case
			when refill_date_next > next_row_order_date_added then refill_date_next + interval '1' day
			else order_date_added + interval '4' month
		end as event_date,
		'PATIENT_CHURNED_OTHER' as event_name
	from all_dates
	where
		coalesce(refill_date_next + interval '1' day, order_date_added + interval '4' month) < next_row_order_date_added
		and order_date_shipped < next_row_order_date_added
		and order_date_added + interval '4' month < next_row_order_date_added
		and (has_refills and rx_date_expired >= coalesce(refill_date_next, order_date_added + interval '4' month))
		and (not patient_inactive or coalesce(refill_date_next, order_date_added + interval '4' month) < patient_date_changed)
	union
	select distinct
		patient_id_cp,
		case
			when refill_date_next > next_row_order_date_added then refill_date_next + interval '1' day
			else order_date_added + interval '4' month
		end as event_date,
		'PATIENT_CHURNED_NO_FILLABLE_RX' as event_name
	from all_dates
	where
		coalesce(refill_date_next + interval '1' day, order_date_added + interval '4' month) < next_row_order_date_added
		and order_date_shipped < next_row_order_date_added
		and order_date_added + interval '4' month < next_row_order_date_added
		and not (has_refills and rx_date_expired >= coalesce(refill_date_next, order_date_added + interval '4' month))
		and (not patient_inactive or coalesce(refill_date_next, order_date_added + interval '4' month) < patient_date_changed)
),
statuses as (
	select
		gp.patient_id_cp,
		patient_date_added as event_date,
		'PATIENT_UNREGISTERED' as event_name
		from "datawarehouse".goodpill."patients" gp
		inner join (
			select distinct patient_id_cp from "datawarehouse".goodpill."rxs_joined" rxs
		) t using (patient_id_cp)
		where patient_date_added is not null
			and (patient_date_registered is null or date(patient_date_registered) >= date(patient_date_added))
	union
	select
		gp.patient_id_cp,
		patient_date_added as event_date,
		'PATIENT_NO_RX' as event_name
		from "datawarehouse".goodpill."patients" gp
		left join (
			select
				patient_id_cp,
				min(rx_date_added) as min_rx_date_added
			from "datawarehouse".goodpill."rxs_joined" rxs
			group by patient_id_cp
		) r on r.patient_id_cp = gp.patient_id_cp
		where patient_date_registered is not null
			and (r.patient_id_cp is null or patient_date_added < min_rx_date_added)
	union
	select
		patient_id_cp,
		patient_date_updated as event_date,
		'PATIENT_INACTIVE' as event_name
		from "datawarehouse".goodpill."patients"
		where patient_inactive = 'Inactive'
	union
	select
		patient_id_cp,
		patient_date_updated as event_date,
		'PATIENT_DECEASED' as event_name
		from "datawarehouse".goodpill."patients"
		where patient_inactive = 'Deceased'
)

select
	patient_id_cp,
	event_name,
	event_date,
	md5(cast(concat(event_name, patient_id_cp, event_date) as TEXT)) as unique_event_id
from (
	select *
	from calc_statuses
	union
	select *
	from statuses
) gps
order by patient_id_cp, event_name, event_date desc