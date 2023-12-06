#!/usr/bin/python3
import datetime
import dateutil.parser as dateparser
import psycopg2
import pandas as pd
import openpyxl
import os # инструменты работы с операционной системой


# -------------------------------------------Создание подключения к PostgreSQL------------------------------------------
# Подключение к базе 'edu'
conn_edu = psycopg2.connect(database = 'edu',
                            host = 'de-edu-db',
                            user = 'deaian', password = 'deaian',
                            port = '5432'
                            )

conn_edu.autocommit = False
cursor_edu = conn_edu.cursor()

# Подключение к базе 'bank'
conn_bank = psycopg2.connect(database = 'bank',
                            host = 'de-edu-db',
                            user = 'bank_etl', password = 'bank_etl',
                            port = '5432'
                            )

conn_bank.autocommit = False
cursor_bank = conn_bank.cursor()


# ---------------------------------------------Очистка staging-таблиц---------------------------------------------------
cursor_edu.execute(f"""SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_schema='deaian' 
                        and table_name like 'lapp_stg_%'
                    """)
records = cursor_edu.fetchall()  # переменная со всеми записями таблиц
list_names = [row[0] for row in records]  # переменная cо списком наименований таблиц

for table_stg in list_names:
    cursor_edu.execute(f"DELETE FROM deaian.{table_stg}")


# -----------------------------Подготовка данных и захват данных из источника в staging---------------------------------

# Функция для получения списка названий колонок таблицы
def get_list_columns(table_name):
    cursor_edu.execute(f"""SELECT column_name
                                ,data_type
                            FROM information_schema.columns
                            WHERE table_name = '{table_name}'
                        """)
    records = cursor_edu.fetchall()
    columns = [row[0] for row in records]
    return columns

# функция для загрузки данных из таблицы источника в staging
def df_to_stg(df_table, stg_table, is_from_files = False):
    columns = get_list_columns(stg_table)
    df_table.columns = columns
    df_table = df_table[columns]
    values_text = ('%s, ' * len(columns)).rstrip(', ') # переменная со списком из знаков '%s' по количеству колонок в таблице, последнюю запятую убираем
    cursor_edu.executemany(f"""INSERT INTO deaian.{stg_table}{tuple(columns)}
                                VALUES({values_text})""".replace("'", ""), df_table.values.tolist()
                           )

    if is_from_files:
        # Переименовываем прочитанные файлы в .backup и переносим в архив
        os.rename(f'/home/deaian/lapp/project/{file}', f'/home/deaian/lapp/project/archive/{file_to_backup}.backup')


# -------------------------------------------Обработка таблиц из базы 'bank'--------------------------------------------

# Функция для создания DataFrame из таблиц базы 'bank'
def get_df_from_bank(table_bank):
    cursor_bank.execute(f"SELECT * FROM info.{table_bank}")
    records = cursor_bank.fetchall()  # переменная с записями таблиц
    columns = [name[0] for name in cursor_bank.description]  # переменная c наименованиями колонок таблиц
    return pd.DataFrame(data=records, columns=columns)

# Переменные с DataFrame из таблиц базы 'bank'
df_clients = get_df_from_bank('clients')
df_accounts = get_df_from_bank('accounts')
df_cards = get_df_from_bank('cards')

# Прописываем недостающие столбцы для загрузки в staging
df_clients['processed_dt'] = datetime.datetime.now()
df_accounts['processed_dt'] = datetime.datetime.now()
df_cards['processed_dt'] = datetime.datetime.now()

# Вызов функции для загрузки данных из таблиц 'bank' в staging
df_to_stg(df_clients, 'lapp_stg_clients')
df_to_stg(df_accounts, 'lapp_stg_accounts')
df_to_stg(df_cards, 'lapp_stg_cards')


# --------------------------------------------------Обработка файлов----------------------------------------------------

# Поиск файлов в папке
allow_formtas = ['csv', 'txt', 'xlsx'] # переменная с допустимым расширением файлов
dir = "/home/deaian/lapp/project/" # папка в которой поиск файлов

for _, _, files in os.walk(dir):
    for file in files:
        format = file.split('.')[1]
        if format not in allow_formtas:
            continue
        path = dir + '/' + file # путь к файлу
        splited_name = file.split('_')[-1].split('.')[0]  # делим строчку по _, берем последний элемент, делим по . и берем первый (0)
        date_file = datetime.datetime.strptime(splited_name, '%d%m%Y')  # дата файла
        file_to_backup = file.split('.')[0]  # имя файла без расширения

        # Прописываем способ чтения файла в зависимости от его расширения:
        if 'transactions' and ('.txt' or '.csv') in file:
            df_transactions = pd.read_csv(path, sep=';', decimal=',', encoding='utf8')
            df_to_stg(df_transactions, 'lapp_stg_transactions', True)

        elif 'terminals' in file and '.xlsx' in file:
            df_terminals = pd.read_excel(path, header=0, index_col='terminal_id').reset_index()
            if 'create_dt' not in df_terminals.columns:
                df_terminals['create_dt'] = date_file
            if 'update_dt' not in df_terminals.columns:
                df_terminals['update_dt'] = None
            df_terminals['processed_dt'] = datetime.datetime.now()
            df_to_stg(df_terminals, 'lapp_stg_terminals', True)

        elif 'passport_blacklist' and '.xlsx' in file:
            df_passport_blacklist = pd.read_excel(path, header=0, index_col=None)
            df_to_stg(df_passport_blacklist, 'lapp_stg_blacklist', True)

        else:
            continue


# ---------------------------------- Применение данных в приемник DDS --------------------------------------------------
# ----------------------------------------------------Вставка-----------------------------------------------------------

def insert_to_table(dim_table, stg_table, id):
    list_dim_columns = get_list_columns(dim_table)
    list_stg_columns = get_list_columns(stg_table)
    list_stg_columns = ['stg.' + direction for direction in list_stg_columns] # добавляем алиасы
    dim_columns = str(list_dim_columns).replace('[', '').replace(']', '').replace("'", "").replace(', tgt.create_dt, tgt.update_dt, tgt.processed_dt', '')
    stg_columns_insert = str(list_stg_columns).replace('[', '').replace(']', '').replace("'", "").replace(', stg.create_dt, stg.update_dt, stg.processed_dt', '')

    cursor_edu.execute(f"""INSERT INTO deaian.{dim_table}({dim_columns})
                            SELECT
                                {stg_columns_insert}
                                ,coalesce(stg.update_dt, stg.create_dt)
                                ,null
                                ,now()
                            FROM deaian.{stg_table} stg
                            LEFT JOIN deaian.{dim_table} tgt
                            on stg.{id} = tgt.{id}
                            WHERE tgt.{id} is null
                        """)

insert_to_table('lapp_dwh_dim_terminals', 'lapp_stg_terminals', 'terminal_id')
insert_to_table('lapp_dwh_dim_clients', 'lapp_stg_clients', 'client_id')
insert_to_table('lapp_dwh_dim_accounts', 'lapp_stg_accounts', 'account_num')
insert_to_table('lapp_dwh_dim_cards', 'lapp_stg_cards', 'card_num')


# ---------------------------------------Заполнение таблиц stg_del------------------------------------------------------

def table_stg_del(del_table, stg_table):
    list_columns = get_list_columns(del_table)
    del_columns = str(list_columns).replace('[', '').replace(']', '').replace("'", "")
    cursor_edu.execute(f"""INSERT INTO deaian.{del_table}({del_columns})
                          SELECT {del_columns}
                          FROM deaian.{stg_table}
                        """)

table_stg_del('lapp_stg_terminals_del', 'lapp_stg_terminals')
table_stg_del('lapp_stg_clients_del', 'lapp_stg_clients')
table_stg_del('lapp_stg_accounts_del', 'lapp_stg_accounts')
table_stg_del('lapp_stg_cards_del', 'lapp_stg_cards')


# ---------------------------------------------------Обновление---------------------------------------------------------
# ---------------------------------------------------terminals----------------------------------------------------------

cursor_edu.execute("""UPDATE deaian.lapp_dwh_dim_terminals
                        set
                            terminal_id = tmp.terminal_id, 
                            terminal_type = tmp.terminal_type, 
                            terminal_city = tmp.terminal_city, 
                            terminal_address = tmp.terminal_address,
                            update_dt = tmp.update_dt,
                            processed_dt = now()
                        FROM (
                            SELECT
                                stg.terminal_id, 
                                stg.terminal_type, 
                                stg.terminal_city, 
                                stg.terminal_address,
                                stg.update_dt 
                          FROM deaian.lapp_stg_terminals stg
                          INNER JOIN deaian.lapp_dwh_dim_terminals tgt
                          on stg.terminal_id = tgt.terminal_id
                          WHERE stg.terminal_type <> tgt.terminal_type 
                                or (stg.terminal_type is null and tgt.terminal_type is not null) 
                                or (stg.terminal_type is not null and tgt.terminal_type is null)
                                or stg.terminal_city <> tgt.terminal_city 
                                or (stg.terminal_city is null and tgt.terminal_city is not null) 
                                or (stg.terminal_city is not null and tgt.terminal_city is null)
                                or stg.terminal_address <> tgt.terminal_address 
                                or (stg.terminal_address is null and tgt.terminal_address is not null) 
                                or (stg.terminal_address is not null and tgt.terminal_address is null)
                            ) tmp
                        WHERE lapp_dwh_dim_terminals.terminal_id = tmp.terminal_id
                    """)

# -----------------------------------------------------clients----------------------------------------------------------

cursor_edu.execute("""UPDATE deaian.lapp_dwh_dim_clients
                        set 
                            client_id = tmp.client_id
                            ,last_name = tmp.last_name
                            ,first_name = tmp.first_name
                            ,patronymic = tmp.patronymic
                            ,date_of_birth = tmp.date_of_birth
                            ,passport_num = tmp.passport_num
                            ,passport_valid_to = tmp.passport_valid_to
                            ,phone = tmp.phone
                            ,update_dt = tmp.update_dt
                            ,processed_dt = now()
                        FROM (
                            SELECT
                                stg.client_id
                                ,stg.last_name
                                ,stg.first_name
                                ,stg.patronymic
                                ,stg.date_of_birth
                                ,stg.passport_num
                                ,stg.passport_valid_to
                                ,stg.phone
                                ,stg.update_dt
                            FROM deaian.lapp_stg_clients stg
                            INNER JOIN deaian.lapp_dwh_dim_clients tgt
                            on stg.client_id = tgt.client_id
                            WHERE stg.client_id <> tgt.client_id 
                                or (stg.client_id is null and tgt.client_id is not null) 
                                or (stg.client_id is not null and tgt.client_id is null)
                                or stg.last_name <> tgt.last_name 
                                or (stg.last_name is null and tgt.last_name is not null) 
                                or (stg.last_name is not null and tgt.last_name is null)
                                or stg.first_name <> tgt.first_name 
                                or (stg.first_name is null and tgt.first_name is not null) 
                                or (stg.first_name is not null and tgt.first_name is null)
                                or stg.patronymic <> tgt.patronymic 
                                or (stg.patronymic is null and tgt.patronymic is not null) 
                                or (stg.patronymic is not null and tgt.patronymic is null)
                                or stg.date_of_birth <> tgt.date_of_birth 
                                or (stg.date_of_birth is null and tgt.date_of_birth is not null) 
                                or (stg.date_of_birth is not null and tgt.date_of_birth is null)
                                or stg.passport_num <> tgt.passport_num 
                                or (stg.passport_num is null and tgt.passport_num is not null) 
                                or (stg.passport_num is not null and tgt.passport_num is null)
                                or stg.passport_valid_to <> tgt.passport_valid_to 
                                or (stg.passport_valid_to is null and tgt.passport_valid_to is not null) 
                                or (stg.passport_valid_to is not null and tgt.passport_valid_to is null)
                                or stg.phone <> tgt.phone 
                                or (stg.phone is null and tgt.phone is not null) 
                                or (stg.phone is not null and tgt.phone is null)
                                ) tmp
                        WHERE lapp_dwh_dim_clients.client_id = tmp.client_id
                    """)

# ----------------------------------------------------accounts----------------------------------------------------------

cursor_edu.execute("""UPDATE deaian.lapp_dwh_dim_accounts
                        set 
                            account_num = tmp.account_num
                            ,valid_to = tmp.valid_to
                            ,client = tmp.client
                            ,update_dt = tmp.update_dt
                            ,processed_dt = now()
                        FROM (
                            SELECT
                                stg.account_num
                                ,stg.valid_to
                                ,stg.client
                                ,stg.update_dt
                            FROM deaian.lapp_stg_accounts stg
                            INNER JOIN deaian.lapp_dwh_dim_accounts tgt
                            on stg.account_num = tgt.account_num
                            WHERE stg.account_num <> tgt.account_num 
                                or (stg.account_num is null and tgt.account_num is not null) 
                                or (stg.account_num is not null and tgt.account_num is null)
                                or stg.valid_to <> tgt.valid_to 
                                or (stg.valid_to is null and tgt.valid_to is not null) 
                                or (stg.valid_to is not null and tgt.valid_to is null)
                                or stg.client <> tgt.client 
                                or (stg.client is null and tgt.client is not null) 
                                or (stg.client is not null and tgt.client is null)
                                ) tmp
                        WHERE lapp_dwh_dim_accounts.account_num = tmp.account_num
                    """)

# ------------------------------------------------------cards-----------------------------------------------------------

cursor_edu.execute("""UPDATE deaian.lapp_dwh_dim_cards
                        set card_num = tmp.card_num 
                            ,account_num = tmp.account_num
                            ,update_dt = tmp.update_dt
                            ,processed_dt = now()
                        FROM (
                            SELECT
                                stg.card_num
                                ,stg.account_num
                                ,stg.update_dt 
                            FROM deaian.lapp_stg_cards stg
                            INNER JOIN deaian.lapp_dwh_dim_cards tgt
                            on stg.card_num = tgt.card_num
                            WHERE stg.card_num <> tgt.card_num 
                                or (stg.card_num is null and tgt.card_num is not null) 
                                or (stg.card_num is not null and tgt.card_num is null)
                                or stg.account_num <> tgt.account_num 
                                or (stg.account_num is null and tgt.account_num is not null) 
                                or (stg.account_num is not null and tgt.account_num is null)
                                ) tmp
                        WHERE lapp_dwh_dim_cards.card_num = tmp.card_num
                    """)


# ----------------------------------------------------Удаление----------------------------------------------------------

def delete_table(dim_table, id, del_table):
    cursor_edu.execute(f"""DELETE FROM deaian.{dim_table}
                            WHERE {id} in (
                            SELECT tgt.{id}
                            FROM deaian.{dim_table} tgt
                            LEFT JOIN deaian.{del_table} stg
                            on tgt.{id} = stg.{id}
                            WHERE stg.{id} is null
                            )
                        """)

delete_table('lapp_dwh_dim_terminals','terminal_id','lapp_stg_terminals_del')
delete_table('lapp_dwh_dim_clients','client_id','lapp_stg_clients_del')
delete_table('lapp_dwh_dim_accounts','account_num','lapp_stg_accounts_del')
delete_table('lapp_dwh_dim_cards','card_num','lapp_stg_cards_del')


# -------------------------------Сохраняем состояние загрузки в метаданные----------------------------------------------

def save_to_meta(dim_table, stg_table):
    cursor_edu.execute(f"""INSERT INTO deaian.lapp_meta (schema_name, table_name, max_update_dt)
                            SELECT
                                'deaian'
                                ,'{dim_table}'
                                ,to_date('1900-01-01','YYYY-MM-DD')
                            WHERE not exists (SELECT * FROM deaian.lapp_meta 
                                                WHERE schema_name = 'deaian' 
                                                and table_name = '{dim_table}'
                                                )
                        """)

    cursor_edu.execute(f"""UPDATE deaian.lapp_meta
                            set 
                                max_update_dt = (SELECT coalesce(max(update_dt), max(create_dt))
                                                 FROM deaian.{stg_table}
                                                 )
                            WHERE schema_name = 'deaian' and table_name = '{dim_table}'
                        """)

save_to_meta('lapp_dwh_dim_terminals', 'lapp_stg_terminals')
save_to_meta('lapp_dwh_dim_clients', 'lapp_stg_clients')
save_to_meta('lapp_dwh_dim_accounts', 'lapp_stg_accounts')
save_to_meta('lapp_dwh_dim_cards', 'lapp_stg_cards')


# -----------------------------------------Загрузка фактовых таблиц-----------------------------------------------------

def insert_fact_table(fact_table, stg_table, id, dt):
    list_fact_columns = get_list_columns(fact_table)
    list_stg_columns = get_list_columns(stg_table)
    list_stg_columns = ['stg.' + direction for direction in list_stg_columns] # добавляем алиасы
    fact_columns = str(list_fact_columns).replace('[', '').replace(']', '').replace("'", "")
    stg_columns_insert = str(list_stg_columns).replace('[', '').replace(']', '').replace("'", "")
    cursor_edu.execute(f"""INSERT INTO deaian.{fact_table} ({fact_columns})
                            SELECT
                                {stg_columns_insert}
                            FROM deaian.{stg_table} stg
                            LEFT JOIN deaian.{fact_table} fact
                            on stg.{id} = fact.{id}
                            WHERE fact.{dt} is null
                        """)
insert_fact_table('lapp_dwh_fact_passport_blacklist', 'lapp_stg_blacklist', 'passport_num', 'entry_dt')
insert_fact_table('lapp_dwh_fact_transactions', 'lapp_stg_transactions', 'trans_id', 'trans_date')


# --------------------------------------------ПОСТРОЕНИЕ ОТЧЕТОВ--------------------------------------------------------

# 1 тип мошенничества (cовершение операции при просроченном или заблокированном паспорте)
cursor_edu.execute("""INSERT INTO deaian.lapp_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
                        SELECT 
                            trans_date
                            ,passport_num
                            ,last_name ||  ' ' || first_name || ' ' || patronymic as fio
                            ,phone
                            ,1 as event_type
                            ,to_date(to_char(trans_date, 'YYYY-MM-DD'), 'YYYY-MM-DD') as report_dt
                        FROM deaian.lapp_dwh_fact_transactions ft
                        LEFT JOIN deaian.lapp_dwh_dim_cards dca 
                        on trim(ft.card_num) = trim(dca.card_num)
                        LEFT JOIN deaian.lapp_dwh_dim_accounts da
                        on dca.account_num = da.account_num
                        LEFT JOIN deaian.lapp_dwh_dim_clients dcl 
                        on da.client = dcl.client_id
                        WHERE 1=1
                        or passport_valid_to < trans_date and passport_valid_to is not null
                        or passport_num in (SELECT passport_num FROM lapp_dwh_fact_passport_blacklist)
                        """)

# 2 тип мошенничества (cовершение операции при недействующем договоре)
cursor_edu.execute("""INSERT INTO deaian.lapp_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
                        SELECT 
                                trans_date
                                ,passport_num
                                ,last_name ||  ' ' || first_name || ' ' || patronymic as fio
                                ,phone
                                ,2 as event_type
                                ,to_date(to_char(trans_date, 'YYYY-MM-DD'), 'YYYY-MM-DD') as report_dt
                        FROM deaian.lapp_dwh_fact_transactions ft
                        LEFT JOIN deaian.lapp_dwh_dim_cards dc 
                        on trim(ft.card_num) = trim(dc.card_num)
                        LEFT JOIN deaian.lapp_dwh_dim_accounts da
                        on dc.account_num = da.account_num
                        LEFT JOIN deaian.lapp_dwh_dim_clients ddc
                        on da.client = ddc.client_id 
                        WHERE da.valid_to < trans_date
                    """)

# 3 тип мошенничества (cовершение операций в разных городах в течение одного часа)
cursor_edu.execute("""WITH pre_tab as (SELECT
                                            ft.trans_id
                                            ,terminal_city
                                            ,lag(terminal_city) over(partition by dc.card_num order by trans_date) prev_city
                                            ,extract(epoch FROM trans_date - lag(trans_date) over(partition by dc.card_num order by trans_date))/3600 hours
                                    FROM deaian.lapp_dwh_fact_transactions ft
                                    LEFT JOIN deaian.lapp_dwh_dim_cards dc
                                    on trim(ft.card_num) = trim(dc.card_num)
                                    LEFT JOIN lapp_dwh_dim_terminals dt
                                    on ft.terminal = dt.terminal_id
                                    )
                        INSERT INTO deaian.lapp_rep_fraud(event_dt, passport, fio, phone, event_type, report_dt)
                        SELECT 
                            trans_date
                            ,passport_num
                            ,last_name || ' ' || first_name || ' ' || patronymic as fio
                            ,phone
                            ,3 as event_type
                             ,to_date(to_char(trans_date, 'YYYY-MM-DD'), 'YYYY-MM-DD') as report_dt
                        FROM deaian.lapp_dwh_fact_transactions ft
                        LEFT JOIN deaian.lapp_dwh_dim_cards dc 
                        on replace(ft.card_num, ' ', '') = replace(dc.card_num, ' ', '')
                        LEFT JOIN deaian.lapp_dwh_dim_accounts da
                        on dc.account_num = da.account_num
                        LEFT JOIN deaian.lapp_dwh_dim_clients ddc
                        on da.client = ddc.client_id
                        WHERE trans_id in (SELECT trans_id 
                        FROM pre_tab
                        WHERE terminal_city <> prev_city and hours < 1
                                    )
                    """)



# Завершаем транзакцию
conn_edu.commit()

# Закрываем соединение
cursor_edu.close()
conn_edu.close()

cursor_bank.close()
conn_bank.close()
