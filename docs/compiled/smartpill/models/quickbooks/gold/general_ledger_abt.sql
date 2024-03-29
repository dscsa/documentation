
with  __dbt__cte__int__purchase_double_entry as (
with purchase_join as (
    with purchases as (
        select distinct on (p.id)
            p.*
        from "datawarehouse".quickbooks."purchases" p

        left join "datawarehouse".quickbooks."deleted_objects" del on object_type = 'Purchase' and p.id = del.id
        where del.id is null or p.updated_at > del.updated_at

        order by p.id, p._airbyte_emitted_at desc
    ),

    purchase_lines as (
        select *
        from "datawarehouse".quickbooks."purchases_lines"
    ),

    items_stg as (
        select distinct on (item.id)
            item.*
        from "datawarehouse".quickbooks."items" item

        left join "datawarehouse".quickbooks."deleted_objects" del on object_type = 'Item' and item.id = del.id
        where del.id is null or item.updated_at > del.updated_at

        order by item.id, item._airbyte_emitted_at desc
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
        purchase_lines.amount * purchases.exchange_rate as amount,
        coalesce(purchase_lines.account_expense_account_id, items.parent_expense_account_id, items.expense_account_id) as payed_to_account_id,
        purchases.account_id as payed_from_account_id,
        case when coalesce(purchases.credit, 'false') = 'true' then 'debit' else 'credit' end as payed_from_transaction_type,
        case when coalesce(purchases.credit, 'false') = 'true' then 'credit' else 'debit' end as payed_to_transaction_type,
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
        class_id,
        customer_id
    from purchase_join
)

select *
from final
),  __dbt__cte__int__deposit_double_entry as (
with deposit_join as (
    with deposits as (
        select distinct on (d.id)
            d.*
        from "datawarehouse".quickbooks."deposits" d
        left join "datawarehouse".quickbooks."deleted_objects" del on object_type = 'Deposit' and d.id = del.id
        where del.id is null or d.updated_at > del.updated_at
        order by id, _airbyte_emitted_at desc
    ),

    deposit_lines as (
        select *
        from "datawarehouse".quickbooks."deposits_lines"
    ),

    accounts as (
        select distinct on (id)
            *
        from "datawarehouse".quickbooks."accounts"
        order by id, _airbyte_emitted_at desc
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
        deposit_lines.amount * deposits.exchange_rate as amount,
        deposits.account_id as deposit_to_acct_id,
        coalesce(deposit_lines.account_id, uf_accounts.id) as deposit_from_acct_id,
        customer_id as customer_id,
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
        class_id,
        customer_id
    from deposit_join
)

select *
from final
),  __dbt__cte__int__journal_entry_double_entry as (
/*
Table that provides the debit and credit records of a journal entry transaction.
*/
with journal_entries as (
    select distinct on (j.id)
        j.*
    from "datawarehouse".quickbooks."journal_entries" j

    left join "datawarehouse".quickbooks."deleted_objects" del on object_type = 'JournalEntry' and j.id = del.id
    where del.id is null or j.updated_at > del.updated_at

    order by j.id, j._airbyte_emitted_at desc
),

journal_entry_lines as (
    select *
    from "datawarehouse".quickbooks."journal_entries_lines"
),

final as (
    select
        journal_entries.id as transaction_id,
        journal_entries.transaction_date,
        -- journal_entry_lines.vendor_id,
        journal_entry_lines.amount * journal_entries.exchange_rate as amount,
        journal_entry_lines.account_id,
        lower(journal_entry_lines.posting_type) as transaction_type,
        'journal_entry' as transaction_source,
        journal_entry_lines.class_id,
        journal_entry_lines.customer_id
    from journal_entries

    inner join journal_entry_lines
        on journal_entries._hash_id = journal_entry_lines._journal_entry_hash_id

    where journal_entry_lines.amount is not null
)

select *
from final
),  __dbt__cte__int__payment_double_entry as (
with payment as (
    select distinct on (p.id)
        p.id as transaction_id,
        transaction_date,
        total_amount * exchange_rate as amount,
        deposit_to_account_id,
        receivable_account_id,
        customer_id as customer_id
    from "datawarehouse".quickbooks."payments" p

    left join "datawarehouse".quickbooks."deleted_objects" del on object_type = 'Payment' and p.id = del.id
    where del.id is null or p.updated_at > del.updated_at

    order by p.id, p._airbyte_emitted_at desc
),

ar_accounts as (
    select
        id
    from "datawarehouse".quickbooks."accounts"
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
        deposit_to_account_id as account_id,
        'debit' as transaction_type,
        'payment' as transaction_source,
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
        null as class_id,
        customer_id
    from payment

    cross join ar_accounts
)

select *
from final
),  __dbt__cte__int__bill_double_entry as (
with bill_join as (
    with bills as (
        select distinct on (b.id)
            b.*
        from "datawarehouse".quickbooks."bills" b
        left join "datawarehouse".quickbooks."deleted_objects" del on object_type = 'Bill' and b.id = del.id
        where del.id is null or b.updated_at > del.updated_at
        order by b.id, b._airbyte_emitted_at desc
    ),

    bill_lines as (
        select *
        from "datawarehouse".quickbooks."bills_lines"
    ),

    items_stg as (
        select distinct on (i.id)
            i.*
        from "datawarehouse".quickbooks."items" i 
        where id not in (select id from "datawarehouse".quickbooks."deleted_objects" del where object_type = 'Item' and i.updated_at <= del.updated_at)
        order by i.id, i._airbyte_emitted_at desc
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
        bill_lines.amount * bills.exchange_rate as amount,
        coalesce(bill_lines.account_expense_account_id, items.expense_account_id, items.parent_expense_account_id, items.expense_account_id, items.parent_income_account_id, items.income_account_id) as payed_to_account_id,
        bills.payable_account_id,
        coalesce(bill_lines.account_expense_customer_id, bill_lines.item_expense_customer_id) as customer_id,
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
        class_id,
        customer_id
    from bill_join
)

select *
from final
),  __dbt__cte__int__invoice_double_entry as (
with invoice_join as (
    with invoices as (
        select distinct on (i.id)
            i.*
        from "datawarehouse".quickbooks."invoices" i
        left join "datawarehouse".quickbooks."deleted_objects" del on object_type = 'Invoice' and i.id = del.id
        where del.id is null or i.updated_at > del.updated_at
        order by i.id, i._airbyte_emitted_at desc
    ),

    invoice_lines as (
        select *
        from "datawarehouse".quickbooks."invoices_lines"
    ),

    items_stg as (
        select distinct on (i.id)
            i.*
        from "datawarehouse".quickbooks."items" i
        left join "datawarehouse".quickbooks."deleted_objects" del on object_type = 'Item' and i.id = del.id
        where del.id is null or i.updated_at > del.updated_at
        order by i.id, i._airbyte_emitted_at desc
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
        select distinct on (id)
            *
        from "datawarehouse".quickbooks."accounts"
        order by id, _airbyte_emitted_at desc
    )

    select
        invoices.id as transaction_id,
        invoices.transaction_date as transaction_date,
        case when invoices.total_amount != 0
            then invoice_lines.amount * invoices.exchange_rate
            else invoices.total_amount * invoices.exchange_rate
                end as amount,

        coalesce(items.income_account_id) as account_id,

        invoices.customer_id,
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
    from "datawarehouse".quickbooks."accounts"

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
        class_id,
        customer_id
    from invoice_join

    cross join ar_accounts
)

select *
from final
),  __dbt__cte__int__bill_payment_double_entry as (
with bill_payment_join as (
    with bill_payments as (
        select distinct on (bp.id)
            bp.*
        from "datawarehouse".quickbooks."bill_payments" bp
        left join "datawarehouse".quickbooks."deleted_objects" del on object_type = 'BillPayment' and bp.id = del.id
        where del.id is null or bp.updated_at > del.updated_at
        order by bp.id, bp._airbyte_emitted_at desc
    ),

    accounts as (
        select distinct on (id)
            *
        from "datawarehouse".quickbooks."accounts"
        order by id, _airbyte_emitted_at desc
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
        bill_payments.total_amount * bill_payments.exchange_rate as amount,
        coalesce(bill_payments.credit_card_account_id,bill_payments.check_bank_account_id) as payment_account_id,
        ap_accounts.account_id
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
        null as class_id,
        null as customer_id
    from bill_payment_join
)

select *
from final
),  __dbt__cte__int__payroll_check_double_entry as (
with payroll_check as (
    select
        id as transaction_id,
        transaction_date,
        amount,
        account_number,
        account_fully_qualified_name
    from "datawarehouse"."raw".quickbooks_payroll_checks
),

accounts as (
    select distinct on (id)
        id,
        account_number,
        fully_qualified_name
    from "datawarehouse".quickbooks."accounts"
    order by id, _airbyte_emitted_at
),

final as (
    select
        transaction_id,
        transaction_date,
        -- customer_id,
        -- cast(null as int) as vendor_id,
        amount,
        a.id as account_id,
        'debit' as transaction_type,
        'payroll check' as transaction_source,
        null as class_id,
        null as customer_id
    from payroll_check pc
    inner join accounts a on pc.account_number = a.account_number or pc.account_fully_qualified_name = a.fully_qualified_name
)

select *
from final
),  __dbt__cte__int__sales_receipts_double_entry as (
with sales_receipts as (
    select distinct on (s.id)
        s.*
    from "datawarehouse".quickbooks."sales_receipts" s

    left join "datawarehouse".quickbooks."deleted_objects" del on object_type = 'SalesReceipt' and s.id = del.id
    where del.id is null or s.updated_at > del.updated_at

    order by s.id, s._airbyte_emitted_at desc
),

sales_receipt_lines as (
    select
        *
    from "datawarehouse".quickbooks."sales_receipts_lines"
),

items as (
    select distinct on (item.id)
        item.*,
        parent.income_account_id as parent_income_account_id
    from "datawarehouse".quickbooks."items" item
    left join "datawarehouse".quickbooks."items" parent
        on item.parent_item_id = parent.id

    left join "datawarehouse".quickbooks."deleted_objects" del on object_type = 'Item' and item.id = del.id
    where del.id is null or item.updated_at > del.updated_at

    order by item.id, item._airbyte_emitted_at desc
),

sales_receipt_join as (
    select
        sales_receipts.id as transaction_id,
        sales_receipts.transaction_date,
        sales_receipt_lines.amount * sales_receipts.exchange_rate as amount,
        sales_receipts.deposit_to_account_id as debit_to_account_id,
        coalesce(items.parent_income_account_id, items.income_account_id) as credit_to_account_id,
        sales_receipts.customer_id,
        sales_receipt_lines.sales_item_class_id as class_id
    from sales_receipts

    inner join sales_receipt_lines
        on sales_receipts._hash_id = sales_receipt_lines._sales_receipts_hash_id

    left join items
        on sales_receipt_lines.sales_item_item_id = items.id

    where coalesce(sales_receipt_lines.sales_item_item_id) is not null
),

final as (
    select
        transaction_id,
        transaction_date,
        amount,
        debit_to_account_id as account_id,
        'debit' as transaction_type,
        'sales_receipt' as transaction_source,
        class_id,
        customer_id
    from sales_receipt_join

    union all

    select
        transaction_id,
        transaction_date,
        amount,
        credit_to_account_id as account_id,
        'credit' as transaction_type,
        'sales_receipt' as transaction_source,
        class_id,
        customer_id
    from sales_receipt_join
)

select *
from final
),  __dbt__cte__int__credit_memo_double_entry as (
with credit_memos as (
    select distinct on (c.id)
        c.*
    from "datawarehouse".quickbooks."credit_memos" c
    where id not in (select id from "datawarehouse".quickbooks."deleted_objects" del where object_type = 'CreditMemo' and c.updated_at <= del.updated_at)
    order by c.id, c._airbyte_emitted_at desc
),

credit_memo_lines as (
    select *
    from "datawarehouse".quickbooks."credit_memos_lines"
),

items as (
    select distinct on (i.id) 
        i.*
    from "datawarehouse".quickbooks."items" i 
    left join "datawarehouse".quickbooks."deleted_objects" del on object_type = 'Item' and i.id = del.id
    where del.id is null or i.updated_at > del.updated_at
    order by i.id, i._airbyte_emitted_at desc
),

accounts as (
    select distinct on (id)
        *
    from "datawarehouse".quickbooks."accounts"
    order by id, _airbyte_emitted_at desc
),

df_accounts as (
    select
        id as account_id
    from accounts

    where account_type = 'Accounts Receivable'
        and is_active
),

credit_memo_join as (
    select
        credit_memos.id as transaction_id,
        credit_memos.transaction_date,
        credit_memo_lines.amount * credit_memos.exchange_rate as amount,
        coalesce(credit_memo_lines.sales_item_account_id, items.income_account_id, items.expense_account_id) as account_id,
        credit_memos.customer_id,
        credit_memo_lines.sales_item_class_id as class_id
    from credit_memos

    inner join credit_memo_lines
        on credit_memos._hash_id = credit_memo_lines._credit_memo_hash_id

    left join items
        on credit_memo_lines.sales_item_item_id = items.id

    where coalesce(credit_memo_lines.sales_item_account_id, credit_memo_lines.sales_item_item_id) is not null
),

final as (
    select
        transaction_id,
        transaction_date,
        amount * -1 as amount,
        account_id,
        'credit' as transaction_type,
        'credit_memo' as transaction_source,
        class_id,
        customer_id
    from credit_memo_join

    union all

    select 
        transaction_id,
        transaction_date,
        amount * -1 as amount,
        df_accounts.account_id,
        'debit' as transaction_type,
        'credit_memo' as transaction_source,
        class_id,
        customer_id
    from credit_memo_join

    cross join df_accounts
)

select *
from final
),  __dbt__cte__int__transfer_double_entry as (
with transfers as (
    select distinct on (t.id)
        t.*
    from "datawarehouse".quickbooks."transfers" t

    left join "datawarehouse".quickbooks."deleted_objects" del on object_type = 'Transfer' and t.id = del.id
    where del.id is null or t.updated_at > del.updated_at

    order by t.id, t._airbyte_emitted_at desc
),

transfer_body as (
    select
        id as transaction_id,
        transaction_date,
        amount * exchange_rate as amount,
        from_account_id as credit_to_account_id,
        to_account_id as debit_to_account_id
    from transfers
),

final as (
    select 
        transaction_id,
        transaction_date,
        amount,
        credit_to_account_id as account_id,
        'credit' as transaction_type,
        'transfer' as transaction_source,
        null::varchar as class_id,
        null::varchar as customer_id
    from transfer_body

    union all

    select 
        transaction_id,
        transaction_date,
        amount,
        debit_to_account_id as account_id,
        'debit' as transaction_type,
        'transfer' as transaction_source,
        null::varchar as class_id,
        null::varchar as customer_id
    from transfer_body
)

select *
from final
),  __dbt__cte__int__vendor_credit_double_entry as (
with vendor_credits as (
    select distinct on (v.id)
        v.*
    from "datawarehouse".quickbooks."vendor_credits" v

    left join "datawarehouse".quickbooks."deleted_objects" del on object_type = 'VendorCredit' and v.id = del.id
    where del.id is null or v.updated_at > del.updated_at

    order by v.id, v._airbyte_emitted_at desc
),

vendor_credit_lines as (
    select *
    from "datawarehouse".quickbooks."vendor_credits_lines"
),

vendor_credit_join as (
    select
        vendor_credits.id as transaction_id,
        vendor_credits.transaction_date,
        vendor_credit_lines.amount * vendor_credits.exchange_rate as amount,
        vendor_credit_lines.account_expense_class_id as class_id,
        vendor_credits.payable_account_id as debit_to_account_id,
        vendor_credit_lines.account_expense_account_id as credit_account_id,
        account_expense_customer_id as customer_id,
        vendor_credits.vendor_id
    from vendor_credits
    
    inner join vendor_credit_lines 
        on vendor_credits._hash_id = vendor_credit_lines._vendor_credit_hash_id
),

final as (
    select 
        transaction_id,
        transaction_date,
        amount,
        credit_account_id as account_id,
        'credit' as transaction_type,
        'vendor_credit' as transaction_source,
        class_id,
        customer_id
    from vendor_credit_join

    union all

    select 
        transaction_id,
        transaction_date,
        amount,
        debit_to_account_id as account_id,
        'debit' as transaction_type,
        'vendor_credit' as transaction_source,
        class_id,
        customer_id
    from vendor_credit_join
)

select *
from final
),  __dbt__cte__int__account_classifications as (
with accounts as (
    select distinct on (id)
        *
    from "datawarehouse".quickbooks."accounts"
    order by id, _airbyte_emitted_at desc
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
where financial_statement_helper = 'income_statement' and classification is not null
),gl_union as (
    select
        transaction_id,
        transaction_date,
        -- cast(null as int) as vendor_id,
        amount,
        account_id,
        transaction_type,
        transaction_source,
        class_id,
        customer_id
    from __dbt__cte__int__purchase_double_entry

    union all

    select *
    from __dbt__cte__int__deposit_double_entry

    union all

    select *
    from __dbt__cte__int__journal_entry_double_entry

    union all

    select *
    from __dbt__cte__int__payment_double_entry

    union all

    select *
    from __dbt__cte__int__bill_double_entry

    union all

    select *
    from __dbt__cte__int__invoice_double_entry

    union all

    select *
    from __dbt__cte__int__bill_payment_double_entry

    union all

    select *
    from __dbt__cte__int__payroll_check_double_entry

    union all

    select *
    from __dbt__cte__int__sales_receipts_double_entry

    union all

    select *
    from __dbt__cte__int__credit_memo_double_entry

    union all

    select *
    from __dbt__cte__int__transfer_double_entry

    union all

    select *
    from __dbt__cte__int__vendor_credit_double_entry
),

accounts as (
    select *
    from __dbt__cte__int__account_classifications
),


qgl as (
    select
        gl_union.transaction_id,
        row_number() over(partition by gl_union.transaction_id order by gl_union.transaction_date) as transaction_index,
        gl_union.transaction_date,
        gl_union.account_id,
        accounts.financial_statement_helper as report_type,
        accounts.classification as account_type_top,
        gl_union.transaction_type,
        gl_union.transaction_source,
        gl_union.class_id,
        gl_union.customer_id,
        accounts.transaction_type as account_transaction_type,
        case when accounts.transaction_type = gl_union.transaction_type
            then gl_union.amount
            else gl_union.amount * -1
                end as amount
    from gl_union

    inner join accounts
        on gl_union.account_id = accounts.id

    where accounts.classification is not null
),

qcl as (
    select distinct on (id)
        *
    from "datawarehouse".quickbooks."classes"
    order by id, _airbyte_emitted_at desc
),

qcu as (
    select distinct on (id)
        *
    from "datawarehouse".quickbooks."customers"
    order by id, _airbyte_emitted_at desc
)

select
    qgl.*,
    qcl.fully_qualified_name as class_full,
    qcl.name as class,
    qa.name as account_sub,
    qa.fully_qualified_name as account_full,
    qa.account_type as account_type_sub,
    qa.account_number as account_number,
    qa.top_level_id as account_top_id,
    qa.parent_account_id as account_parent_id,
    qap.name as account_top,
    qap.account_number as account_top_number,
    qcu.display_name as customer_display_name,
    qcu.balance as customer_balance,
    qcu.company_name as customer_company_name
from qgl
left join qcl on (qcl.id = qgl.class_id)
left join "datawarehouse".quickbooks."accounts_top_level" qa on (qa.id = qgl.account_id)
left join "datawarehouse".quickbooks."accounts_top_level" qap on (qap.id = qa.top_level_id)
left join qcu on (qcu.id = qgl.customer_id)