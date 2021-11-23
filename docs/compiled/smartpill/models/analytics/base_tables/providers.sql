select
    rxs1.provider_npi,
    rxs1.provider_first_name,
    rxs1.provider_last_name,
    rxs1.provider_phone,
    NOW() as date_processed
from analytics_v2.goodpill_gp_rxs_single rxs1
    left join analytics_v2.goodpill_gp_rxs_single rxs2
    on (rxs1.provider_npi = rxs2.provider_npi and rxs1.rx_number < rxs2.rx_number)
where rxs2.rx_number is null and rxs1.provider_npi is not null and rxs1.provider_npi <> ''

	and rxs1._airbyte_emitted_at > (select MAX(date_processed) from analytics.`providers`)
