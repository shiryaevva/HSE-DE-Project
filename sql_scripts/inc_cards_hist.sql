-- 1. Загрузка в приемник "вставок" на источнике (формат SCD2).
insert into public.shrv_dwh_dim_cards_hist( card_num, account_num, effective_from, effective_to, deleted_flg )
select 
    stg.card_num
    , stg.account_num
    , stg.update_dt
    , to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
	, False
from public.shrv_stg_cards stg
    left join public.shrv_dwh_dim_cards_hist tgt on stg.card_num = tgt.card_num
where true
    and tgt.card_num is null;

-- 2. Обновление в приемнике "обновлений" на источнике (формат SCD2).
drop table if exists shrv_tmp_cards;
create temporary table shrv_tmp_cards as (
    select 
		stg.card_num
        , stg.account_num
        , stg.update_dt
	from public.shrv_stg_cards stg
	    inner join public.shrv_dwh_dim_cards_hist tgt on stg.card_num = tgt.card_num
	where true
        and (stg.account_num != tgt.account_num 
            or (stg.account_num is null and tgt.account_num is not null) 
            or (stg.account_num is not null and tgt.account_num is null)
        )
        and tgt.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp);

update public.shrv_dwh_dim_cards_hist
set effective_to = tmp.update_dt - interval '1 second' second
from shrv_tmp_cards tmp
where true 
    and shrv_dwh_dim_cards_hist.card_num = tmp.card_num
    and shrv_dwh_dim_cards_hist.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp;

insert into public.shrv_dwh_dim_cards_hist( card_num, account_num, effective_from, effective_to, deleted_flg )
select 
    tmp.card_num
    , tmp.account_num
    , tmp.update_dt
    , to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
    , False
from shrv_tmp_cards tmp;

drop table if exists shrv_tmp_cards;

-- 3. Удаление в приемнике удаленных в источнике записей (формат SCD2).
drop table if exists shrv_tmp_cards_del;
create temporary table shrv_tmp_cards_del as (
	select 
        tgt.card_num
        , tgt.account_num
        , (select max(update_dt) from public.shrv_stg_cards) as deleted_dt
	from public.shrv_stg_cards_del stg
	    right join public.shrv_dwh_dim_cards_hist tgt on stg.card_num = tgt.card_num
	where true
        and stg.card_num is null
        and tgt.deleted_flg != True
        and tgt.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
);

update public.shrv_dwh_dim_cards_hist
set effective_to = tmp.deleted_dt - interval '1 second'
from shrv_tmp_cards_del tmp
where true 
    and shrv_dwh_dim_cards_hist.card_num = tmp.card_num
    and shrv_dwh_dim_cards_hist.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp;

insert into public.shrv_dwh_dim_cards_hist( card_num, account_num, effective_from, effective_to, deleted_flg )
select 
    tmp.card_num
    , tmp.account_num
    , tmp.deleted_dt
    , to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
    , True
from shrv_tmp_cards_del tmp;

drop table if exists shrv_tmp_cards_del;

-- 4. Обновление метаданных.
update public.shrv_meta_dwh
set max_update_dt = coalesce((select max(effective_from) from public.shrv_dwh_dim_cards_hist),
                             (select max(max_update_dt) from public.shrv_meta_dwh where schema_name = 'public' and table_name = 'shrv_dwh_dim_cards_hist'))
where true
    and schema_name = 'public' 
    and table_name = 'shrv_dwh_dim_cards_hist';

-- 5. Фиксация транзакции.
commit;