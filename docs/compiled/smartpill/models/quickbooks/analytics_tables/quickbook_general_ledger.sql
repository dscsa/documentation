

with  __dbt__CTE__int_quickbooks__purchase_double_entry as (
with purchase_join as (
    with purchases as (
        select * from analytics.`quickbook_purchases`
    where id in (
        select id
        from analytics.`quickbook_purchases`
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
    ),

    purchase_lines as (
        select *
        from analytics.`quickbook_purchases_lines`
    ),

    items_stg as (
        select * from analytics.`quickbook_items`
    where id in (
        select id
        from analytics.`quickbook_items`
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
    ),

    items as (
        select 
            item.*, 
            parent.expense_account_id as parent_expense_account_id,
            parent.income_account_id as parent_income_account_id
        from items_stg item

        left join items_stg parent
            on item.parent_item_id = parent.id

    )

    select
        purchases.id as transaction_id,
        purchases.transaction_date,
        purchase_lines.amount,
        coalesce(purchase_lines.account_expense_account_id, items.parent_expense_account_id, items.expense_account_id) as payed_to_account_id,
        purchases.account_id as payed_from_account_id,
        case when coalesce(purchases.credit, false) = true then 'debit' else 'credit' end as payed_from_transaction_type,
        case when coalesce(purchases.credit, false) = true then 'credit' else 'debit' end as payed_to_transaction_type,
        purchases.currency_name,
        account_expense_class_id as class_id,
        coalesce(purchases.customer_id, purchase_lines.account_expense_customer_id) as customer_id
    from purchases
    
    inner join purchase_lines
        on purchases._hash_id = purchase_lines._purchase_hash_id

    left join items
        on purchase_lines.item_expense_item_id = items.id
),

final as (
    select
        transaction_id,
        transaction_date,
        amount,
        payed_from_account_id as account_id,
        payed_from_transaction_type as transaction_type,
        'purchase' as transaction_source,
        currency_name,
        class_id,
        customer_id
    from purchase_join

    union all

    select
        transaction_id,
        transaction_date,
        amount,
        payed_to_account_id as account_id,
        payed_to_transaction_type as transaction_type,
        'purchase' as transaction_source,
        currency_name,
        class_id,
        customer_id
    from purchase_join
)

select *
from final
),  __dbt__CTE__int_quickbooks__deposit_double_entry as (
with deposit_join as (
    with deposits as (
        select * from analytics.`quickbook_deposits`
    where id in (
        select id
        from analytics.`quickbook_deposits`
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
    ),

    deposit_lines as (
        select *
        from analytics.`quickbook_deposits_lines`
    ),

    accounts as (
        select * from analytics.`quickbook_accounts`
    where id in (
        select id
        from analytics.`quickbook_accounts`
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
    ),

    uf_accounts as (
        select
            id
        from accounts

        where account_sub_type = 'UndepositedFunds'
            and is_active
    )

    select
        deposits.id as transaction_id,
        deposits.transaction_date,
        deposit_lines.amount,
        deposits.account_id as deposit_to_acct_id,
        coalesce(deposit_lines.account_id, uf_accounts.id) as deposit_from_acct_id,
        customer_id as customer_id,
        currency_name,
        deposit_lines.class_id
    from deposits
    
    inner join deposit_lines 
        on deposits._hash_id = deposit_lines._deposit_hash_id

    cross join uf_accounts
),

final as (
    select
        transaction_id,
        transaction_date,
        -- customer_id,
        -- cast(null as int) as vendor_id,
        amount,
        deposit_to_acct_id as account_id,
        'debit' as transaction_type,
        'deposit' as transaction_source,
        deposit_join.currency_name,
        class_id,
        customer_id
    from deposit_join

    union all

    select
        transaction_id,
        transaction_date,
        -- customer_id,
        -- cast(null as int) as vendor_id,
        amount,
        deposit_from_acct_id as account_id,
        'credit' as transaction_type,
        'deposit' as transaction_source,
        currency_name,
        class_id,
        customer_id
    from deposit_join
)

select *
from final
),  __dbt__CTE__int_quickbooks__journal_entry_double_entry as (
/*
Table that provides the debit and credit records of a journal entry transaction.
*/
with journal_entries as (
    select * from analytics.`quickbook_journal_entries`
    where id in (
        select id
        from analytics.`quickbook_journal_entries`
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
),

journal_entry_lines as (
    select *
    from analytics.`quickbook_journal_entries_lines`
),

final as (
    select
        journal_entries.id as transaction_id,
        journal_entries.transaction_date,
        -- journal_entry_lines.vendor_id,
        journal_entry_lines.amount,
        journal_entry_lines.account_id,
        lower(journal_entry_lines.posting_type) as transaction_type,
        'journal_entry' as transaction_source,
        journal_entries.currency_name,
        journal_entry_lines.class_id,
        journal_entry_lines.customer_id
    from journal_entries

    inner join journal_entry_lines
        on journal_entries._hash_id = journal_entry_lines._journal_entry_hash_id

    where journal_entry_lines.amount is not null
)

select *
from final
),  __dbt__CTE__int_quickbooks__payment_double_entry as (
with payment as (
    select
        id as transaction_id,
        transaction_date,
        total_amount as amount,
        deposit_to_account_id,
        receivable_account_id,
        customer_id as customer_id,
        currency_name
    from analytics.`quickbook_payments`
    where id in (
        select id
        from analytics.`quickbook_payments`
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
),

ar_accounts as (
    select
        id
    from analytics.`quickbook_accounts`

    where account_type = 'Accounts Receivable'
        and is_active
    limit 1
),

final as (
    select
        transaction_id,
        transaction_date,
        -- customer_id,
        -- cast(null as int) as vendor_id,
        amount,
        deposit_to_account_id as account_id,
        'debit' as transaction_type,
        'payment' as transaction_source,
        payment.currency_name,
        null as class_id,
        customer_id
    from payment

    union all

    select
        transaction_id,
        transaction_date,
        -- customer_id,
        -- cast(null as int) as vendor_id,
        amount,
        coalesce(receivable_account_id, ar_accounts.id) as account_id,
        'credit' as transaction_type,
        'payment' as transaction_source,
        payment.currency_name,
        null as class_id,
        customer_id
    from payment
    
    cross join ar_accounts
)

select *
from final
),  __dbt__CTE__int_quickbooks__bill_double_entry as (
with bill_join as (
    with bills as (
        select * from analytics.`quickbook_bills`
    where id in (
        select id
        from analytics.`quickbook_bills`
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
    ),

    bill_lines as (
        select *
        from analytics.`quickbook_bills_lines`
    ),

    items_stg as (
        select * from analytics.`quickbook_items`
    where id in (
        select id
        from analytics.`quickbook_items`
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
    ),

    items as (
        select 
            item.*, 
            parent.expense_account_id as parent_expense_account_id,
            parent.income_account_id as parent_income_account_id
        from items_stg item

        left join items_stg parent
            on item.parent_item_id = parent.id

    )

    select
        bills.id as transaction_id, 
        bills.transaction_date,
        bill_lines.amount,
        coalesce(bill_lines.account_expense_account_id, items.expense_account_id, items.parent_expense_account_id, items.expense_account_id, items.parent_income_account_id, items.income_account_id) as payed_to_account_id,
        bills.payable_account_id,
        coalesce(bill_lines.account_expense_customer_id, bill_lines.item_expense_customer_id) as customer_id,
        bills.currency_name,
        coalesce(bill_lines.account_expense_class_id, bill_lines.item_expense_class_id) as class_id
        -- bills.vendor_id
    from bills
    
    inner join bill_lines
        on bills._hash_id = bill_lines._bill_hash_id

    left join items
        on bill_lines.item_expense_item_id = items.id
),

final as (
    select 
        transaction_id,
        transaction_date,
        -- vendor_id,
        amount,
        payed_to_account_id as account_id,
        'debit' as transaction_type,
        'bill' as transaction_source,
        currency_name,
        class_id,
        customer_id
    from bill_join

    union all

    select
        transaction_id,
        transaction_date,
        -- vendor_id,
        amount,
        payable_account_id as account_id,
        'credit' as transaction_type,
        'bill' as transaction_source,
        currency_name,
        class_id,
        customer_id
    from bill_join
)

select *
from final
),  __dbt__CTE__int_quickbooks__invoice_double_entry as (
with invoice_join as (
    with invoices as (
        select * from analytics.`quickbook_invoices`
    where id in (
        select id
        from analytics.`quickbook_invoices`
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
    ),

    invoice_lines as (
        select *
        from analytics.`quickbook_invoices_lines`
    ),

    items_stg as (
        select * from analytics.`quickbook_items`
    where id in (
        select id
        from analytics.`quickbook_items`
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
    ),

    items as (
        select 
            item.*, 
            parent.expense_account_id as parent_expense_account_id,
            parent.income_account_id as parent_income_account_id
        from items_stg item

        left join items_stg parent
            on item.parent_item_id = parent.id

    ),

    accounts as (
        select *
        from analytics.`quickbook_accounts`
        where id in (
        select id
        from analytics.`quickbook_accounts`
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
    )

    select
        invoices.id as transaction_id,
        invoices.transaction_date as transaction_date,
        case when invoices.total_amount != 0
            then invoice_lines.amount
            else invoices.total_amount
                end as amount,

        coalesce(items.income_account_id) as account_id,

        invoices.customer_id,
        invoices.currency_name,
        invoice_lines.sales_item_class_id as class_id

    from invoices

    inner join invoice_lines
        on invoices._hash_id = invoice_lines._invoice_hash_id

    left join items
        on invoice_lines.sales_item_item_id = items.id

    where coalesce(invoice_lines.sales_item_account_id, invoice_lines.sales_item_item_id) is not null 

),

ar_accounts as (
    select *
    from analytics.`quickbook_accounts`

    where account_type = 'Accounts Receivable'
    limit 1
),

final as (
    select
        transaction_id,
        transaction_date,
        -- customer_id,
        -- cast(null as int) as vendor_id,
        amount,
        account_id,
        'credit' as transaction_type,
        'invoice' as transaction_source,
        invoice_join.currency_name,
        class_id,
        customer_id
    from invoice_join

    union all

    select
        transaction_id,
        transaction_date,
        -- customer_id,
        -- cast(null as int) as vendor_id,
        amount,
        ar_accounts.id as account_id,
        'debit' as transaction_type,
        'invoice' as transaction_source,
        invoice_join.currency_name,
        class_id,
        customer_id
    from invoice_join

    cross join ar_accounts
)

select * 
from final
),  __dbt__CTE__int_quickbooks__bill_payment_double_entry as (
with bill_payment_join as (
    with bill_payments as (
        select * from analytics.`quickbook_bill_payments`
    where id in (
        select id
        from analytics.`quickbook_bill_payments`
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
    ),

    accounts as (
        select * from analytics.`quickbook_accounts`
    where id in (
        select id
        from analytics.`quickbook_accounts`
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
    ),

    ap_accounts as (
        select
            id as account_id
        from accounts
        
        where account_type = 'Accounts Payable'
            and is_active
    )

    select
        bill_payments.id as transaction_id,
        bill_payments.transaction_date,
        bill_payments.total_amount as amount,
        coalesce(bill_payments.credit_card_account_id,bill_payments.check_bank_account_id) as payment_account_id,
        ap_accounts.account_id,
        bill_payments.currency_name
        -- bill_payments.vendor_id
    from bill_payments

    cross join ap_accounts
),

final as (
    select
        transaction_id,
        transaction_date,
        -- cast(null as int) as customer_id,
        -- vendor_id,
        amount,
        payment_account_id as account_id,
        'credit' as transaction_type,
        'bill payment' as transaction_source,
        currency_name,
        null as class_id,
        null as customer_id
    from bill_payment_join

    union all

    select
        transaction_id,
        transaction_date,
        -- cast(null as int) as customer_id,
        -- vendor_id,
        amount,
        account_id,
        'debit' as transaction_type,
        'bill payment' as transaction_source,
        currency_name,
        null as class_id,
        null as customer_id
    from bill_payment_join
)

select *
from final
),  __dbt__CTE__int_quickbooks__payroll_check_double_entry as (
with payroll_check as (
    select
        id as transaction_id,
        transaction_date,
        total_amount as amount,
        deposit_to_account_id
    from analytics_v2.quickbook_payroll_checks
),

final as (
    select
        transaction_id,
        transaction_date,
        -- customer_id,
        -- cast(null as int) as vendor_id,
        amount,
        deposit_to_account_id as account_id,
        'debit' as transaction_type,
        'payroll check' as transaction_source,
        'United States Dollar',
        null as class_id,
        null as customer_id
    from payroll_check
)

select *
from final
),  __dbt__CTE__int_quickbooks__account_classifications as (
with accounts as (
    select * from analytics.`quickbook_accounts`
    where id in (
        select id
        from analytics.`quickbook_accounts`
        group by id
        having _airbyte_emitted_at = max(_airbyte_emitted_at)
    )
),

classification_fix as (
    select 
        id,
        account_number,
        is_sub_account,
        parent_account_id,
        name,
        account_type,
        account_sub_type,
        balance,
        balance_with_sub_accounts,
        is_active,
        created_at,
        currency_name,
        description,
        fully_qualified_name,
        updated_at,
        case when classification is not null
            then classification
            when classification is null and account_type in ('Bank', 'Other Current Asset', 'Fixed Asset', 'Other Asset', 'Accounts Receivable')
                then 'Asset'
            when classification is null and account_type = 'Equity'
                then 'Equity'
            when classification is null and account_type in ('Expense', 'Other Expense', 'Cost of Goods Sold')
                then 'Expense'
            when classification is null and account_type in ('Accounts Payable', 'Credit Card', 'Long Term Liability', 'Other Current Liability')
                then 'Liability'
            when classification is null and account_type in ('Income', 'Other Income')
                then 'Revenue'
                    end as classification
    from accounts
),

classification_add as (
    select
        *,
        case when classification in ('Liability', 'Equity')
            then -1
        when classification = 'Asset'
            then 1
            else null
                end as multiplier,
        case when classification in ('Asset', 'Liability', 'Equity')
            then 'balance_sheet'
            else 'income_statement'
                end as financial_statement_helper,
        case when classification in ('Asset', 'Expense')
            then 'debit'
            else 'credit'
                end as transaction_type
    from classification_fix
),

adjusted_balances as (
    select 
        *,
        (balance * multiplier) as adjusted_balance
    from classification_add
),

final as (
    select
        adjusted_balances.*,
        case when adjusted_balances.is_sub_account
            then parent_accounts.account_number
            else adjusted_balances.account_number
                end as parent_account_number,
        case when adjusted_balances.is_sub_account
            then parent_accounts.fully_qualified_name
            else adjusted_balances.fully_qualified_name
                end as parent_account_name
    from adjusted_balances

    left join accounts as parent_accounts
        on parent_accounts.id = adjusted_balances.parent_account_id
)

select *
from final
),gl_union as (
    select
        transaction_id,
        transaction_date,
        -- cast(null as int) as vendor_id,
        amount,
        account_id,
        transaction_type,
        transaction_source,
        currency_name,
        class_id,
        customer_id
    from __dbt__CTE__int_quickbooks__purchase_double_entry

    union all

    select *
    from __dbt__CTE__int_quickbooks__deposit_double_entry

    union all

    select *
    from __dbt__CTE__int_quickbooks__journal_entry_double_entry

    union all

    select *
    from __dbt__CTE__int_quickbooks__payment_double_entry

    union all

    select *
    from __dbt__CTE__int_quickbooks__bill_double_entry

    union all

    select *
    from __dbt__CTE__int_quickbooks__invoice_double_entry

    union all

    select *
    from __dbt__CTE__int_quickbooks__bill_payment_double_entry

    union all

    select *
    from __dbt__CTE__int_quickbooks__payroll_check_double_entry
),

accounts as (
    select *
    from __dbt__CTE__int_quickbooks__account_classifications
),


adjusted_gl as (
    select
        gl_union.transaction_id,
        row_number() over(partition by gl_union.transaction_id order by gl_union.transaction_date) as transaction_index,
        gl_union.transaction_date,
        gl_union.amount,
        gl_union.account_id,
        -- accounts.name as account_name,
        -- accounts.is_sub_account,
        -- accounts.account_type,
        -- accounts.account_sub_type,
        accounts.financial_statement_helper,
        -- accounts.balance as account_current_balance,
        accounts.classification as account_classification, 
        gl_union.transaction_type,
        gl_union.transaction_source,
        gl_union.currency_name,
        gl_union.class_id,
        gl_union.customer_id,
        accounts.transaction_type as account_transaction_type,
        case when accounts.transaction_type = gl_union.transaction_type
            then gl_union.amount
            else gl_union.amount * -1
                end as adjusted_amount
    from gl_union

    left join accounts
        on gl_union.account_id = accounts.id
),

final as (
    select
        *,
        sum(adjusted_amount) over (partition by account_id order by transaction_date, account_id rows unbounded preceding) as running_balance
    from adjusted_gl
)

select *
from final