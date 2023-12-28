-- 1. Загрузка в приемник "вставок" на источнике (формат SCD2).
insert into public.shrv_dwh_dim_clients_hist( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg )
select 
    stg.client_id
    , stg.last_name
    , stg.first_name
    , stg.patronymic
    , stg.date_of_birth
    , stg.passport_num
    , stg.passport_valid_to
    , stg.phone
    , stg.update_dt
    , to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
	, False
from public.shrv_stg_clients stg
    left join public.shrv_dwh_dim_clients_hist tgt on stg.client_id = tgt.client_id
where true
    and tgt.client_id is null;

-- 2. Обновление в приемнике "обновлений" на источнике (формат SCD2).
drop table if exists shrv_tmp_clients;
create temporary table shrv_tmp_clients as (
    with except_table as (
        select 
            stg.client_id
            , stg.last_name
            , stg.first_name
            , stg.patronymic
            , stg.date_of_birth
            , stg.passport_num
            , stg.passport_valid_to
            , stg.phone
        from public.shrv_stg_clients stg
        except select
            tgt.client_id
            , tgt.last_name
            , tgt.first_name
            , tgt.patronymic
            , tgt.date_of_birth
            , tgt.passport_num
            , tgt.passport_valid_to
            , tgt.phone
        from public.shrv_dwh_dim_clients_hist tgt
        where true
            and tgt.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
    )
    select
        stg.client_id
        , stg.last_name
        , stg.first_name
        , stg.patronymic
        , stg.date_of_birth
        , stg.passport_num
        , stg.passport_valid_to
        , stg.phone
        , stg.update_dt
    from public.shrv_stg_clients stg
    where stg.client_id in (select client_id from except_table)
 );

update public.shrv_dwh_dim_clients_hist
set effective_to = tmp.update_dt - interval '1 second' second
from shrv_tmp_clients tmp
where true 
    and shrv_dwh_dim_clients_hist.client_id = tmp.client_id
    and shrv_dwh_dim_clients_hist.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp;

insert into public.shrv_dwh_dim_clients_hist( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg )
select 
    tmp.client_id
    , tmp.last_name
    , tmp.first_name
    , tmp.patronymic
    , tmp.date_of_birth
    , tmp.passport_num
    , tmp.passport_valid_to
    , tmp.phone
    , tmp.update_dt
    , to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
    , False
from shrv_tmp_clients tmp;

drop table if exists shrv_tmp_clients;

-- 3. Удаление в приемнике удаленных в источнике записей (формат SCD2).
drop table if exists shrv_tmp_clients_del;
create temporary table shrv_tmp_clients_del as (
	select 
        tgt.client_id
        , tgt.last_name
        , tgt.first_name
        , tgt.patronymic
        , tgt.date_of_birth
        , tgt.passport_num
        , tgt.passport_valid_to
        , tgt.phone
        , (select max(update_dt) from public.shrv_stg_clients) as deleted_dt
	from public.shrv_stg_clients_del stg
	    right join public.shrv_dwh_dim_clients_hist tgt on stg.client_id = tgt.client_id
	where true
        and stg.client_id is null
        and tgt.deleted_flg != True
        and tgt.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
);

update public.shrv_dwh_dim_clients_hist
set effective_to = tmp.deleted_dt - interval '1 second'
from shrv_tmp_clients_del tmp
where true 
    and shrv_dwh_dim_clients_hist.client_id = tmp.client_id
    and shrv_dwh_dim_clients_hist.effective_to = to_date('2999-12-31', 'YYYY-MM-DD')::timestamp;

insert into public.shrv_dwh_dim_clients_hist( client_id, last_name, first_name, patronymic, date_of_birth, passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg )
select 
    tmp.client_id
    , tmp.last_name
    , tmp.first_name
    , tmp.patronymic
    , tmp.date_of_birth
    , tmp.passport_num
    , tmp.passport_valid_to
    , tmp.phone
    , tmp.deleted_dt
    , to_date('2999-12-31', 'YYYY-MM-DD')::timestamp
    , True
from shrv_tmp_clients_del tmp;

drop table if exists shrv_tmp_clients_del;

-- 4. Обновление метаданных.
update public.shrv_meta_dwh
set max_update_dt = coalesce((select max(effective_from) from public.shrv_dwh_dim_clients_hist),
                             (select max(max_update_dt) from public.shrv_meta_dwh where schema_name = 'public' and table_name = 'shrv_dwh_dim_clients_hist'))
where true
    and schema_name = 'public' 
    and table_name = 'shrv_dwh_dim_clients_hist';

-- 5. Фиксация транзакции.
commit;