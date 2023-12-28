-- 1. Загрузка в приемник инкремента.
insert into public.shrv_dwh_fact_transactions( trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal )
select 
    stg.trans_id
    , stg.trans_date
    , stg.card_num
    , stg.oper_type
    , stg.amt
    , stg.oper_result
    , stg.terminal
from public.shrv_stg_transactions stg;

-- 2. Обновление метаданных.
update public.shrv_meta_dwh
set max_update_dt = coalesce((select max(trans_date::date) from public.shrv_stg_transactions),
                             (select max(max_update_dt) from public.shrv_meta_dwh where schema_name = 'public' and table_name = 'shrv_dwh_fact_transactions'))
where true
    and schema_name = 'public' 
    and table_name = 'shrv_dwh_fact_transactions';

-- 3. Фиксация транзакции.
commit;