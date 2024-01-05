-- 1. Собираем широкую витрину с нужными атрибутами.
drop table if exists shrv_tmp_rep;

create temporary table shrv_tmp_rep as (
	with trns_crds as (
	select 
		sdft.trans_date
		, sdft.trans_id 
		, sdft.card_num
		, sdft.oper_type
		, sdft.oper_result
		, sddch.account_num
		, sdft.terminal
		, sdft.amt 
	from public.shrv_dwh_fact_transactions sdft 
		left join public.shrv_dwh_dim_cards_hist sddch on sdft.card_num = sddch.card_num
			and sdft.trans_date between sddch.effective_from and sddch.effective_to
	where sdft.trans_date::date >= (
		select max_update_dt::date - interval '1 hour' 
		from public.shrv_meta_dwh 
		where true
			and schema_name = 'public' 
			and table_name = 'shrv_dwh_fact_transactions'
		) 
	),
	trns_crds_trml as (
	select 
		tc.*
		, sddtm.terminal_city 
	from trns_crds tc 
		left join public.shrv_dwh_dim_terminals_hist sddtm on tc.terminal = sddtm.terminal_id
			and tc.trans_date between sddtm.effective_from and sddtm.effective_to
	),
	trns_crds_trml_clnt as (
	select 
		tc.*
		, sddah.client
		, sddah.valid_to 
	from trns_crds_trml tc 
		left join public.shrv_dwh_dim_accounts_hist sddah on tc.account_num = sddah.account_num
			and tc.trans_date between sddah.effective_from and sddah.effective_to
	)	
	select
		tc.*
		, concat(sddcl.last_name, ' ', sddcl.first_name, ' ', sddcl.patronymic) as fio
		, sddcl.passport_num
		, sddcl.passport_valid_to 
		, sddcl.phone
	from trns_crds_trml_clnt tc
		left join public.shrv_dwh_dim_clients_hist sddcl on tc.client = sddcl.client_id
			and tc.trans_date between sddcl.effective_from and sddcl.effective_to
);


-- 2. Определяем типы мошеннических операций
drop table if exists shrv_tmp_fraud;

create temporary table shrv_tmp_fraud as (
select
	tmp.trans_date as event_dt
	, tmp.passport_num as passport
	, tmp.fio
	, tmp.phone
	, 1 as event_type
	, tmp.trans_date::date as report_dt
from shrv_tmp_rep tmp
where true 
	and tmp.trans_date > tmp.passport_valid_to
	
union all

select
	tmp.trans_date as event_dt
	, tmp.passport_num as passport
	, tmp.fio
	, tmp.phone
	, 1 as event_type
	, tmp.trans_date::date as report_dt
from shrv_tmp_rep tmp
	inner join public.shrv_dwh_fact_passport_blacklist sdfpb on tmp.passport_num = sdfpb.passport_num 
		and tmp.trans_date > sdfpb.entry_dt 
	
union all

select
	tmp.trans_date as event_dt
	, tmp.passport_num as passport
	, tmp.fio
	, tmp.phone
	, 2 as event_type
	, tmp.trans_date::date as report_dt
from shrv_tmp_rep tmp
where true 
	and tmp.trans_date > tmp.valid_to
	
union all

select
	trans_date as event_dt
	, passport_num as passport
	, fio
	, phone
	, 3 as event_type
	, trans_date::date as report_dt
from (
	select 
		tmp.*
		, lag(trans_date) over (partition by card_num order by trans_date asc) as prev_trans_date
		, lag(terminal_city) over (partition by card_num order by trans_date asc) as prev_terminal_city
	from shrv_tmp_rep tmp
	) s
where true 
	and terminal_city != prev_terminal_city
	and trans_date <= prev_trans_date + interval '1 hour'
		
union all

select
	trans_date as event_dt
	, passport_num as passport
	, fio
	, phone
	, 4 as event_type
	, trans_date::date as report_dt
from (
	select
		tmp.*
		, lag(oper_result) over (partition by card_num order by trans_date asc) as prev_oper_result
		, lag(oper_result, 2) over (partition by card_num order by trans_date asc) as prev2_oper_result
		, lag(oper_result, 3) over (partition by card_num order by trans_date asc) as prev3_oper_result
		, lag(amt) over (partition by card_num order by trans_date asc) as prev_amt
		, lag(amt, 2) over (partition by card_num order by trans_date asc) as prev2_amt
		, lag(amt, 3) over (partition by card_num order by trans_date asc) as prev3_amt
		, lag(trans_date, 3) over (partition by card_num order by trans_date asc) as prev3_trans_date
	from shrv_tmp_rep tmp
	where oper_type = 'WITHDRAW'
	) s
where true
	and trans_date <= prev3_trans_date + interval '20 minutes'
	and oper_result = 'SUCCESS' and prev_oper_result = 'REJECT' and prev2_oper_result = 'REJECT' and prev3_oper_result = 'REJECT'
	and amt < prev_amt and prev_amt < prev2_amt and prev2_amt < prev3_amt
);

-- 3. Загрузка инкремента в витрину.
insert into public.shrv_rep_fraud( event_dt, passport, fio, phone, event_type, report_dt)
select
	event_dt
	, passport
	, fio
	, phone
	, event_type
	, report_dt
from shrv_tmp_fraud
where true
	and event_dt::date >= (
		select max_update_dt::date 
		from public.shrv_meta_dwh 
		where true
			and schema_name = 'public' 
			and table_name = 'shrv_dwh_fact_transactions'
		)
order by 1, 3, 2;

-- 4. Обновление метаданных.
update public.shrv_meta_dwh
set max_update_dt = coalesce((select max(report_dt) from shrv_tmp_fraud),
                             (select max(max_update_dt) from public.shrv_meta_dwh where schema_name = 'public' and table_name = 'shrv_rep_fraud'))
where true
    and schema_name = 'public' 
    and table_name = 'shrv_rep_fraud';

-- 5. Фиксация транзакции.
commit;