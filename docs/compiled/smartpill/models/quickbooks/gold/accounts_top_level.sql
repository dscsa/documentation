with recursive accounts as (
     select distinct on (id)
         *
     from "datawarehouse".dev_quickbooks."accounts"
     order by id, _airbyte_emitted_at desc
),
tree as (
    select id,
           parent_account_id,
           name,
           fully_qualified_name,
           account_type,
           account_number,
           1 as level,
           id as top_level_id
   from accounts
   where parent_account_id is null

   union all

   select c.id,
          c.parent_account_id,
          c.name,
          c.fully_qualified_name,
          c.account_type,
          c.account_number,
          t.level + 1,
          coalesce(t.top_level_id, c.id) as top_level_id
   from accounts c
     join tree t on c.parent_account_id = t.id
)
select * from tree