from datetime import datetime
import os
import pandas as pd
import psycopg2
from contextlib import contextmanager


@contextmanager
def get_time():
    try:
        start_time = datetime.now()
        print(f"Started at: {start_time}")
        yield
    finally:
        end_time = datetime.now()
        print(f"Ended at: {end_time}")
        print(f"Duration: {end_time - start_time}", end="\n\n")


def get_sql_query(
    query: str,
    user="hse",
    password="hsepassword",
    host="rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net",
    port="6432",
    database="db",
) -> pd.DataFrame:
    with get_time():
        conn = psycopg2.connect(
            database=database, host=host, user=user, password=password, port=port
        )

        cursor = conn.cursor()
        cursor.execute(query)

        df = pd.DataFrame()
        try:
            df = pd.DataFrame(cursor.fetchall(), columns=[i[0] for i in cursor.description])
        except:
            print('No data to return')

        conn.commit()
        cursor.close()
        conn.close()
        return df


def insert_into_database(
    df: pd.DataFrame,
    table_name: str,
    query: str,
    force=True,
    user="hse",
    password="hsepassword",
    host="rc1b-o3ezvcgz5072sgar.mdb.yandexcloud.net",
    port="6432",
    database="db",
    schema="public",
) -> None:
    with get_time():
        # Создание подключения к PostgreSQL
        conn = psycopg2.connect(
            database=database,
            host=host,
            user=user,
            password=password,
            port=port,
        )

        # Отключение автокоммита
        conn.autocommit = False
        cursor = conn.cursor()

        if force:
            # Очистка
            cursor.execute(f"delete from {schema}.{table_name}")

        cursor.executemany(query, df.values.tolist())

        conn.commit()
        cursor.close()
        conn.close()



def find_first_file(directory, name):
    files = os.listdir(directory)
    min_date = datetime.max.strftime('%Y-%m-%d %H:%M:%S')
    min_date_file = None
    for file in files:
        root, extension = os.path.splitext(file)
        if name in root:
            try:
                file_date = datetime.strptime(root.split('_')[-1], '%d%m%Y').strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                continue

            if file_date < min_date:
                min_date = file_date
                min_date_file = file

    return min_date_file, min_date


def archive_file(path, file):
    new_file_name = file + '.backup'
    new_path = path + 'archive'
    source_path = os.path.join(path, file)
    destination_path = os.path.join(new_path, new_file_name)

    if not os.path.exists(new_path):
        os.makedirs(new_path)

    os.rename(source_path, destination_path)

    print(f"Файл {file} успешно обработан и перемещен в {new_path}")