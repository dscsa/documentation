select distinct 
    provider_clinic as name,
    NOW() as date_processed 
from analytics_v2.goodpill_gp_rxs_single
where provider_clinic IS NOT NULL AND provider_clinic <> ''

	and (select COUNT(*) from analytics.`clinics` where name = provider_clinic) = 0
