-- 1. Загрузка в приемник "вставок" на источнике (формат SCD2).
insert into public.shrv_dwh_dim_terminals_hist( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
select 
    stg.terminal_id
    , stg.terminal_type
    , stg.terminal_city
    , stg.terminal_address
    , stg.update_dt
    , to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
	, False
from public.shrv_stg_terminals stg
    left join public.shrv_dwh_dim_terminals_hist tgt on stg.terminal_id = tgt.terminal_id
where true
    and tgt.terminal_id is null;

-- 2. Обновление в приемнике "обновлений" на источнике (формат SCD2).
drop table if exists shrv_tmp_terminals;
create temporary table shrv_tmp_terminals as (
    with except_table as (
        select 
            stg.terminal_id
            , stg.terminal_type
            , stg.terminal_city
            , stg.terminal_address
        from public.shrv_stg_terminals stg
        except select
            tgt.terminal_id
            , tgt.terminal_type
            , tgt.terminal_city
            , tgt.terminal_address
        from public.shrv_dwh_dim_terminals_hist tgt
        where true
            and tgt.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
    )
    select
        stg.terminal_id
        , stg.terminal_type
        , stg.terminal_city
        , stg.terminal_address
        , stg.update_dt
    from public.shrv_stg_terminals stg
    where stg.terminal_id in (select terminal_id from except_table)
 );

update public.shrv_dwh_dim_terminals_hist
set effective_to = tmp.update_dt - interval '1 second' second
from shrv_tmp_terminals tmp
where true 
    and shrv_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id
    and shrv_dwh_dim_terminals_hist.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp;

insert into public.shrv_dwh_dim_terminals_hist( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
select 
    tmp.terminal_id
    , tmp.terminal_type
    , tmp.terminal_city
    , tmp.terminal_address
    , tmp.update_dt
    , to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
    , False
from shrv_tmp_terminals tmp;

drop table if exists shrv_tmp_terminals;

-- 3. Удаление в приемнике удаленных в источнике записей (формат SCD2).
drop table if exists shrv_tmp_terminals_del;
create temporary table shrv_tmp_terminals_del as (
	select 
        tgt.terminal_id
        , tgt.terminal_type
        , tgt.terminal_city
        , tgt.terminal_address
        , (select max(update_dt) from public.shrv_stg_terminals) as deleted_dt
	from public.shrv_stg_terminals_del stg
	    right join public.shrv_dwh_dim_terminals_hist tgt on stg.terminal_id = tgt.terminal_id
	where true
        and stg.terminal_id is null
        and tgt.deleted_flg != True
        and tgt.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
);

update public.shrv_dwh_dim_terminals_hist
set effective_to = tmp.deleted_dt - interval '1 second'
from shrv_tmp_terminals_del tmp
where true 
    and shrv_dwh_dim_terminals_hist.terminal_id = tmp.terminal_id
    and shrv_dwh_dim_terminals_hist.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp;

insert into public.shrv_dwh_dim_terminals_hist( terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
select 
    tmp.terminal_id
    , tmp.terminal_type
    , tmp.terminal_city
    , tmp.terminal_address
    , tmp.deleted_dt
    , to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
    , True
from shrv_tmp_terminals_del tmp;

drop table if exists shrv_tmp_terminals_del;

-- 4. Обновление метаданных.
update public.shrv_meta_dwh
set max_update_dt = coalesce((select max(effective_from) from public.shrv_dwh_dim_terminals_hist),
                             (select max(max_update_dt) from public.shrv_meta_dwh where schema_name = 'public' and table_name = 'shrv_dwh_dim_terminals_hist'))
where true
    and schema_name = 'public' 
    and table_name = 'shrv_dwh_dim_terminals_hist';

-- 5. Фиксация транзакции.
commit;