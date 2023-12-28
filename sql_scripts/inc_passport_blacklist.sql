-- 1. Загрузка в приемник инкремента.
insert into public.shrv_dwh_fact_passport_blacklist( passport_num, entry_dt )
select 
    stg.passport_num
    , stg.entry_dt
from public.shrv_stg_passport_blacklist stg
    left join public.shrv_dwh_fact_passport_blacklist tgt on stg.passport_num = tgt.passport_num
        and stg.entry_dt = stg.entry_dt
where true
    and tgt.passport_num is null;

-- 2. Обновление метаданных.
update public.shrv_meta_dwh
set max_update_dt = coalesce((select max(update_dt) from public.shrv_stg_passport_blacklist),
                             (select max(max_update_dt) from public.shrv_meta_dwh where schema_name = 'public' and table_name = 'shrv_dwh_fact_passport_blacklist'))
where true
    and schema_name = 'public' 
    and table_name = 'shrv_dwh_fact_passport_blacklist';

-- 3. Фиксация транзакции.
commit;