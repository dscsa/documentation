select
    qgl.running_balance,
    qgl.adjusted_amount,
    qgl.account_transaction_type,
    qgl.transaction_id,
    qgl.transaction_index,
    qgl.transaction_date,
    qgl.amount,
    qgl.account_id,
    qgl.account_classification,
    qgl.transaction_type,
    qgl.transaction_source,
    qgl.currency_name,
    qgl.class_id,
    qgl.customer_id,
    qgl.financial_statement_helper,
    qcl.fully_qualified_name as class_fully_qualified_name,
    qcl.name as class_name,
    qa.name as account_name,
    qa.fully_qualified_name as account_fully_qualified_name,
    qa.account_type as account_type,
    qa.account_number as account_number,
    qa.top_level_id as top_level_account_id,
    qa.parent_account_id as parent_account_id,
    qap.name as top_level_account_name,
    qap.account_type as top_level_account_type,
    qap.account_number as top_level_account_number,
    qcu.display_name as customer_display_name,
    qcu.balance as customer_balance,
    qcu.company_name as customer_company_name
from
    "datawarehouse".analytics."quickbook_general_ledger" qgl
left join (select * from "datawarehouse".analytics."quickbook_classes"
    where id in (
        select id
        from "datawarehouse".analytics."quickbook_classes"
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )) qcl on (qcl.id = qgl.class_id)
left join "datawarehouse".analytics."quickbook_accounts_top_level" qa on (qa.id = qgl.account_id)
left join "datawarehouse".analytics."quickbook_accounts_top_level" qap on (qap.id = qa.top_level_id)
left join (select * from "datawarehouse".analytics."quickbook_customers"
    where id in (
        select id
        from "datawarehouse".analytics."quickbook_customers"
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )) qcu on (qcu.id = qgl.customer_id)