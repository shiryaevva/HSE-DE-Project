-- Создаем таблицы измерений
drop table if exists public.shrv_dwh_dim_accounts_hist;
create table public.shrv_dwh_dim_accounts_hist (
	account_num varchar(50)
	, valid_to date
	, client varchar(20)
	, effective_from timestamp(0)
	, effective_to timestamp(0)
	, deleted_flg boolean
);

drop table if exists public.shrv_dwh_dim_clients_hist;
create table public.shrv_dwh_dim_clients_hist (
	client_id varchar(20)
	, last_name varchar(50)
	, first_name varchar(50)
	, patronymic varchar(50)
	, date_of_birth date
	, passport_num varchar(20)
	, passport_valid_to date
	, phone varchar(20)
	, effective_from timestamp(0)
	, effective_to timestamp(0)
	, deleted_flg boolean
);

drop table if exists public.shrv_dwh_dim_cards_hist;
create table public.shrv_dwh_dim_cards_hist (
	card_num varchar(30)
	, account_num varchar(50)
	, effective_from timestamp(0)
	, effective_to timestamp(0)
	, deleted_flg boolean
);

drop table if exists public.shrv_dwh_dim_terminals_hist;
create table public.shrv_dwh_dim_terminals_hist (
	terminal_id varchar(10)
	, terminal_type varchar(5)
	, terminal_city varchar(50)
	, terminal_address varchar(300)
	, effective_from timestamp(0)
	, effective_to timestamp(0)
	, deleted_flg boolean
);


-- Создаем таблицы фактов
drop table if exists public.shrv_dwh_fact_transactions;
create table public.shrv_dwh_fact_transactions(
	trans_id varchar(20)
	, trans_date timestamp(0)
	, card_num varchar(30)
	, oper_type varchar(20)
	, amt decimal
	, oper_result varchar(20)
	, terminal varchar(10)
);

drop table if exists public.shrv_dwh_fact_passport_blacklist;
create table public.shrv_dwh_fact_passport_blacklist(
	passport_num varchar(20)
	, entry_dt date
);


-- Создаем staging таблицы
drop table if exists public.shrv_stg_accounts;
create table public.shrv_stg_accounts (
	account_num varchar(50)
	, valid_to date
	, client varchar(20)
	, update_dt timestamp(0)
);

drop table if exists public.shrv_stg_accounts_del;
create table public.shrv_stg_accounts_del( 
	account_num varchar(50)
);

drop table if exists public.shrv_stg_clients;
create table public.shrv_stg_clients (
	client_id varchar(20)
	, last_name varchar(50)
	, first_name varchar(50)
	, patronymic varchar(50)
	, date_of_birth date
	, passport_num varchar(20)
	, passport_valid_to date
	, phone varchar(20)
	, update_dt timestamp(0)
);

drop table if exists public.shrv_stg_clients_del;
create table public.shrv_stg_clients_del (
	client_id varchar(20)
);

drop table if exists public.shrv_stg_cards;
create table public.shrv_stg_cards (
	card_num varchar(30)
	, account_num varchar(50)
	, update_dt timestamp(0)
);

drop table if exists public.shrv_stg_cards_del;
create table public.shrv_stg_cards_del (
	card_num varchar(30)
);

drop table if exists public.shrv_stg_terminals;
create table public.shrv_stg_terminals (
	terminal_id varchar(10)
	, terminal_type varchar(5)
	, terminal_city varchar(50)
	, terminal_address varchar(300)
	, update_dt timestamp(0)
);

drop table if exists public.shrv_stg_terminals_del;
create table public.shrv_stg_terminals_del (
	terminal_id varchar(10)
);

drop table if exists public.shrv_stg_transactions;
create table public.shrv_stg_transactions(
	trans_id varchar(20)
	, trans_date timestamp(0)
	, card_num varchar(30)
	, oper_type varchar(20)
	, amt decimal
	, oper_result varchar(20)
	, terminal varchar(10)
);

drop table if exists public.shrv_stg_passport_blacklist;
create table public.shrv_stg_passport_blacklist(
	passport_num varchar(20)
	, entry_dt date
	, update_dt timestamp(0)
);

-- Создаем таблицу метаданных
drop table if exists public.shrv_meta_dwh;
create table public.shrv_meta_dwh (
    schema_name varchar(30)
    , table_name varchar(50)
    , max_update_dt timestamp(0)
);

-- Создаем витрину report
drop table if exists public.shrv_rep_fraud;
create table public.shrv_rep_fraud(
	event_dt timestamp(0)
	, passport varchar(20)
	, fio varchar(150)
	, phone varchar(20)
	, event_type int
	, report_dt timestamp(0)
);

-- Вставляем метаданные
insert into public.shrv_meta_dwh(schema_name, table_name, max_update_dt)
values
    ('public','shrv_dwh_dim_accounts_hist', to_timestamp('1899-01-01','YYYY-MM-DD')),
    ('public','shrv_dwh_dim_clients_hist', to_timestamp('1899-01-01','YYYY-MM-DD')),
    ('public','shrv_dwh_dim_cards_hist', to_timestamp('1899-01-01','YYYY-MM-DD')),
    ('public','shrv_dwh_dim_terminals_hist', to_timestamp('1899-01-01','YYYY-MM-DD')),
    ('public','shrv_dwh_fact_transactions', to_timestamp('1899-01-01','YYYY-MM-DD')),
    ('public','shrv_dwh_fact_passport_blacklist', to_timestamp('1899-01-01','YYYY-MM-DD')),
    ('public','shrv_rep_fraud', to_timestamp('1899-01-01','YYYY-MM-DD'));