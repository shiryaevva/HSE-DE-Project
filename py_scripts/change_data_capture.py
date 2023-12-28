import pandas as pd
from functions import get_sql_query, insert_into_database, find_first_file

def cdc_accounts() -> None:
    accounts_df = get_sql_query("""
    SELECT 
        account AS account_num
        , valid_to
        , client
        , coalesce(update_dt, create_dt) AS update_dt
    FROM info.accounts 
    WHERE true
        AND coalesce(update_dt, create_dt) > (
            SELECT max_update_dt 
            FROM public.shrv_meta_dwh 
            WHERE true
                AND schema_name='public' 
                AND table_name='shrv_dwh_dim_accounts_hist'
        )
    """
    )

    accounts_del_df = get_sql_query("""
    SELECT 
        account AS account_num
    FROM info.accounts 
    """
    )

    for col in accounts_df.columns:
        try:
            accounts_df[col] = accounts_df[col].apply(lambda x: x.strip())
        except:
            continue

    accounts_del_df['account_num'] = accounts_del_df.account_num.astype('str').apply(lambda x: x.strip())

    query = """ 
    INSERT INTO public.shrv_stg_accounts(
        account_num
        , valid_to
        , client
        , update_dt
    ) VALUES(%s, %s, %s, %s) 
    """

    query_del = """ 
    INSERT INTO public.shrv_stg_accounts_del(
        account_num
    ) VALUES(%s) 
    """

    insert_into_database(accounts_df, 'shrv_stg_accounts', query, force=True)
    insert_into_database(accounts_del_df['account_num'].apply(lambda x: (x, )), 'shrv_stg_accounts_del', query_del, force=True)


def cdc_cards() -> None:
    cards_df = get_sql_query("""
    SELECT 
        card_num
        , account AS account_num
        , coalesce(update_dt, create_dt) AS update_dt
    FROM info.cards 
    WHERE true
        AND coalesce(update_dt, create_dt) > (
            SELECT max_update_dt 
            FROM public.shrv_meta_dwh 
            WHERE true
                AND schema_name='public' 
                AND table_name='shrv_dwh_dim_cards_hist'
        )
    """
    )

    cards_del_df = get_sql_query("""
    SELECT 
        card_num
    FROM info.cards 
    """
    )

    for col in cards_df.columns:
        try:
            cards_df[col] = cards_df[col].apply(lambda x: x.strip())
        except:
            continue

    cards_del_df['card_num'] = cards_del_df.card_num.astype('str').apply(lambda x: x.strip())

    query = """ 
    INSERT INTO public.shrv_stg_cards(
        card_num
        , account_num
        , update_dt
    ) VALUES(%s, %s, %s) 
    """

    query_del = """ 
    INSERT INTO public.shrv_stg_cards_del(
        card_num
    ) VALUES(%s) 
    """

    insert_into_database(cards_df, 'shrv_stg_cards', query, force=True)
    insert_into_database(cards_del_df['card_num'].apply(lambda x: (x, )), 'shrv_stg_cards_del', query_del, force=True)


def cdc_clients() -> None:
    clients_df = get_sql_query("""
    SELECT 
        client_id
        , last_name
        , first_name
        , patronymic
        , date_of_birth
        , passport_num
        , passport_valid_to
        , phone
        , coalesce(update_dt, create_dt) AS update_dt
    FROM info.clients 
    WHERE true
        AND coalesce(update_dt, create_dt) > (
            SELECT max_update_dt 
            FROM public.shrv_meta_dwh 
            WHERE true
                AND schema_name='public' 
                AND table_name='shrv_dwh_dim_clients_hist'
        )
    """
    )

    clients_del_df = get_sql_query("""
    SELECT 
        client_id
    FROM info.clients 
    """
    )


    for col in clients_df.columns:
        try:
            clients_df[col] = clients_df[col].apply(lambda x: x.strip())
        except:
            continue
        
    clients_del_df['client_id'] = clients_del_df.client_id.astype('str').apply(lambda x: x.strip())

    query = """ 
    INSERT INTO public.shrv_stg_clients(
        client_id
        , last_name
        , first_name
        , patronymic
        , date_of_birth
        , passport_num
        , passport_valid_to
        , phone
        , update_dt
    ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s) 
    """

    query_del = """ 
    INSERT INTO public.shrv_stg_clients_del(
        client_id
    ) VALUES(%s) 
    """

    insert_into_database(clients_df, 'shrv_stg_clients', query, force=True)
    insert_into_database(clients_del_df['client_id'].apply(lambda x: (x, )), 'shrv_stg_clients_del', query_del, force=True)


def cdc_terminals() -> dict:
    directory_path = '/Users/shiryaevva/HSE/2-nd_year/DE/Project/data/'
    terminals_file, terminals_update = find_first_file(directory_path, 'terminals')
    print(terminals_file, terminals_update, end='\n\n')

    terminals_df = pd.read_excel(f'{directory_path}{terminals_file}')
    terminals_df['update_dt'] = terminals_update
    terminals_df['terminal_id'] = terminals_df['terminal_id'].astype('str')

    for col in terminals_df.columns:
        try:
            terminals_df[col] = terminals_df[col].apply(lambda x: x.strip())
        except:
            continue

    query = """ 
    INSERT INTO public.shrv_stg_terminals(
        terminal_id,
        terminal_type,
        terminal_city,
        terminal_address,
        update_dt
    ) VALUES(%s, %s, %s, %s, %s) 
    """

    query_del = """ 
    INSERT INTO public.shrv_stg_terminals_del(
        terminal_id
    ) VALUES(%s) 
    """

    insert_into_database(terminals_df, 'shrv_stg_terminals', query, force=True)
    insert_into_database(terminals_df['terminal_id'].apply(lambda x: (x, )), 'shrv_stg_terminals_del', query_del, force=True)

    return {'directory_path': directory_path, 'file': terminals_file}


def cdc_transactions() -> dict:
    directory_path = '/Users/shiryaevva/HSE/2-nd_year/DE/Project/data/'
    transactions_file, transactions_update = find_first_file(directory_path, 'transactions')
    print(transactions_file, transactions_update, end='\n\n')

    transactions_df = pd.read_csv(f'{directory_path}{transactions_file}', sep=';')

    for col in transactions_df.columns:
        try:
            transactions_df[col] = transactions_df[col].apply(lambda x: x.strip())
        except:
            continue

    transactions_df['amount'] = transactions_df['amount'].apply(lambda x: x.replace(',', '.')).astype('float')

    query = """
    INSERT INTO public.shrv_stg_transactions(
        trans_id
        , trans_date
        , amt
        , card_num
        , oper_type
        , oper_result
        , terminal
    ) VALUES(%s, %s, %s, %s, %s, %s, %s)
    """

    insert_into_database(transactions_df, 'shrv_stg_transactions', query, force=True)

    return {'directory_path': directory_path, 'file': transactions_file}


def cdc_passport_blacklist() -> dict:
    directory_path = '/Users/shiryaevva/HSE/2-nd_year/DE/Project/data/'
    passport_blacklist_file, passport_blacklist_update = find_first_file(directory_path, 'passport_blacklist')
    print(passport_blacklist_file, passport_blacklist_update, end='\n\n')

    passport_blacklist_df = pd.read_excel(f'{directory_path}{passport_blacklist_file}')
    passport_blacklist_df['update_dt'] = passport_blacklist_update

    for col in passport_blacklist_df.columns:
        try:
            passport_blacklist_df[col] = passport_blacklist_df[col].apply(lambda x: x.strip())
        except:
            continue

    query = """
    INSERT INTO public.shrv_stg_passport_blacklist(
        entry_dt
        , passport_num
        , update_dt
    ) VALUES(%s, %s, %s)
    """

    insert_into_database(passport_blacklist_df, 'shrv_stg_passport_blacklist', query, force=True)

    return {'directory_path': directory_path, 'file': passport_blacklist_file}