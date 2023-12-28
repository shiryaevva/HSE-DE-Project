-- 1. Загрузка в приемник "вставок" на источнике (формат SCD2).
insert into public.shrv_dwh_dim_accounts_hist( account_num, valid_to, client, effective_from, effective_to, deleted_flg )
select 
    stg.account_num
    , stg.valid_to
    , stg.client
    , stg.update_dt
    , to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
	, False
from public.shrv_stg_accounts stg
    left join public.shrv_dwh_dim_accounts_hist tgt on stg.account_num = tgt.account_num
where true
    and tgt.account_num is null;

-- 2. Обновление в приемнике "обновлений" на источнике (формат SCD2).
drop table if exists shrv_tmp_accounts;
create temporary table shrv_tmp_accounts as (
    select 
		stg.account_num
        , stg.valid_to
        , stg.client
        , stg.update_dt
	from public.shrv_stg_accounts stg
	    inner join public.shrv_dwh_dim_accounts_hist tgt on stg.account_num = tgt.account_num
	where true
        and (stg.valid_to != tgt.valid_to 
            or (stg.valid_to is null and tgt.valid_to is not null) 
            or (stg.valid_to is not null and tgt.valid_to is null)
            or stg.client != tgt.client 
            or (stg.client is null and tgt.client is not null) 
            or (stg.client is not null and tgt.client is null)
        )
        and tgt.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp);

update public.shrv_dwh_dim_accounts_hist
set effective_to = tmp.update_dt - interval '1 second' second
from shrv_tmp_accounts tmp
where true 
    and shrv_dwh_dim_accounts_hist.account_num = tmp.account_num
    and shrv_dwh_dim_accounts_hist.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp;

insert into public.shrv_dwh_dim_accounts_hist( account_num, valid_to, client, effective_from, effective_to, deleted_flg )
select 
    tmp.account_num
    , tmp.valid_to
    , tmp.client
    , tmp.update_dt
    , to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
    , False
from shrv_tmp_accounts tmp;

drop table if exists shrv_tmp_accounts;

-- 3. Удаление в приемнике удаленных в источнике записей (формат SCD2).
drop table if exists shrv_tmp_accounts_del;
create temporary table shrv_tmp_accounts_del as (
	select 
        tgt.account_num
        , tgt.valid_to
        , tgt.client
        , (select max(update_dt) from public.shrv_stg_accounts) as deleted_dt
	from public.shrv_stg_accounts_del stg
	    right join public.shrv_dwh_dim_accounts_hist tgt on stg.account_num = tgt.account_num
	where true
        and stg.account_num is null
        and tgt.deleted_flg != True
        and tgt.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
);

update public.shrv_dwh_dim_accounts_hist
set effective_to = tmp.deleted_dt - interval '1 second'
from shrv_tmp_accounts_del tmp
where true 
    and shrv_dwh_dim_accounts_hist.account_num = tmp.account_num
    and shrv_dwh_dim_accounts_hist.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp;

insert into public.shrv_dwh_dim_accounts_hist( account_num, valid_to, client, effective_from, effective_to, deleted_flg )
select 
    tmp.account_num
    , tmp.valid_to
    , tmp.client
    , tmp.deleted_dt
    , to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
    , True
from shrv_tmp_accounts_del tmp;

drop table if exists shrv_tmp_accounts_del;

-- 4. Обновление метаданных.
update public.shrv_meta_dwh
set max_update_dt = coalesce((select max(effective_from) from public.shrv_dwh_dim_accounts_hist),
                             (select max(max_update_dt) from public.shrv_meta_dwh where schema_name = 'public' and table_name = 'shrv_dwh_dim_accounts_hist'))
where true
    and schema_name = 'public' 
    and table_name = 'shrv_dwh_dim_accounts_hist';

-- 5. Фиксация транзакции.
commit;