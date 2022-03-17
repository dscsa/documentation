select
	patient_id_cp,
	event_date as patient_event_date,
	event_name as patient_status,
	
  
    max(
      
      case
      when event_name = 'PATIENT_ACTIVE'
        then event_date
      else null
      end
    )
	
      over(partition by patient_id_cp)
	
    
      
        as date_patient_active
      
    
    ,
  
    max(
      
      case
      when event_name = 'PATIENT_NO_RX'
        then event_date
      else null
      end
    )
	
      over(partition by patient_id_cp)
	
    
      
        as date_patient_no_rx
      
    
    ,
  
    max(
      
      case
      when event_name = 'PATIENT_UNREGISTERED'
        then event_date
      else null
      end
    )
	
      over(partition by patient_id_cp)
	
    
      
        as date_patient_unregistered
      
    
    ,
  
    max(
      
      case
      when event_name = 'PATIENT_CHURNED_NO_FILLABLE_RX'
        then event_date
      else null
      end
    )
	
      over(partition by patient_id_cp)
	
    
      
        as date_patient_churned_no_fillable_rx
      
    
    ,
  
    max(
      
      case
      when event_name = 'PATIENT_DECEASED'
        then event_date
      else null
      end
    )
	
      over(partition by patient_id_cp)
	
    
      
        as date_patient_deceased
      
    
    ,
  
    max(
      
      case
      when event_name = 'PATIENT_INACTIVE'
        then event_date
      else null
      end
    )
	
      over(partition by patient_id_cp)
	
    
      
        as date_patient_inactive
      
    
    ,
  
    max(
      
      case
      when event_name = 'PATIENT_CHURNED_OTHER'
        then event_date
      else null
      end
    )
	
      over(partition by patient_id_cp)
	
    
      
        as date_patient_churned_other
      
    
    
  

from "datawarehouse".dev_analytics."patients_status_historic"