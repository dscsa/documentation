
    
    

with child as (
    select _bill_payment_hash_id as from_field
    from "datawarehouse".dev_analytics."quickbook_bill_payments_lines"
    where _bill_payment_hash_id is not null
),

parent as (
    select _hash_id as to_field
    from "datawarehouse".dev_analytics."quickbook_bill_payments"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


