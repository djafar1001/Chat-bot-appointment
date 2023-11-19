import os.path
import sqlite3 as sq
import pandas as pd
from quickstart import create_table_google_sheets, read_table_google_sheets

async def db_start(): #Создание БД
    global db, cur
    db = sq.connect('appointment.db')
    cur = db.cursor()
    table_name = 'CBAppointment'
    sheet_name = "DB"
    cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    result = cur.fetchone()
    if result is None:
        # data = read_table_google_sheets("Чат-бот встреча", sheet_name)
        data = await read_table_google_sheets("Бот: встреча", sheet_name)
        data['user_ID'] = ""
        data['help_request'] = ""
        data.to_sql(table_name, sq.connect('appointment.db'), index=False)
        ID = pd.read_excel(os.path.abspath("ID.xlsx"))
        ID.to_sql("ID", sq.connect('appointment.db'), index=False)
        IDTM = pd.read_excel(os.path.abspath("IDTM.xlsx"))
        IDTM.to_sql("IDTM", sq.connect('appointment.db'), index=False)
        IDTD = pd.read_excel(os.path.abspath("IDTD.xlsx"))
        IDTD.to_sql("IDTD", sq.connect('appointment.db'), index=False)

        helpR = pd.DataFrame(columns=['ID_help', 'user_ID', 'text_message'])
        helpR.to_sql("Help", sq.connect('appointment.db'), index=False)

    cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='ID'")
    result = cur.fetchone()
    if result is None:
        ID = pd.read_excel(os.path.abspath("ID.xlsx"))
        ID.to_sql("ID", sq.connect('appointment.db'), index=False)
    cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='IDTM'")
    result = cur.fetchone()
    if result is None:
        IDTM = pd.read_excel(os.path.abspath("IDTM.xlsx"))
        IDTM.to_sql("IDTM", sq.connect('appointment.db'), index=False)
    cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='IDTD'")
    result = cur.fetchone()
    if result is None:
        IDTD = pd.read_excel(os.path.abspath("IDTD.xlsx"))
        IDTD.to_sql("IDTD", sq.connect('appointment.db'), index=False)

    table_name = 'Links'
    sheet_name = "Links"
    cur.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
    result = cur.fetchone()
    if result is None:
        # data = read_table_google_sheets("Чат-бот встреча", sheet_name)
        data = await read_table_google_sheets("Бот: встреча", sheet_name)
        data.to_sql(table_name, sq.connect('appointment.db'), index=False)
    db.commit()

async def user_id_from_db(table_name_db, user_id, user_phone): #Вносим номер телефона участника
    sql_update_query = f"""Update {table_name_db} as A  set user_ID = {user_id} 
        where A.ID = (
            select ID
            from ID
            where user_phone = {user_phone})"""
    cur.execute(sql_update_query)
    db.commit()

async def help_from_db(table_name_db, help_request, user_id): #Вносим чат-айди
    sql_update_query = f"""Update {table_name_db} as A set help_request = {help_request} 
    where  user_ID = {user_id}"""
    cur.execute(sql_update_query)
    db.commit()

async def table_help_insert_from_db(user_id, text_message): #Создаем пометку о необходимости помощи
    # cur.execute(f"SELECT MAX(ID_help) FROM Help")
    # result = cur.fetchone()
    # hID = result[0] if not result[0] is None else 0
    df = pd.read_sql(f"SELECT * FROM help",
                     sq.connect('appointment.db'))
    hID = len(df) if not len(df) is None else 0
    text_message = text_message.replace("'", "*")
    text_message = text_message.replace('"', "*")
    sql_update_query = f"""INSERT INTO Help(ID_help, user_ID, text_message) 
                            VALUES ({int(hID)+1}, {user_id}, '{text_message}')"""
    cur.execute(sql_update_query)
    db.commit()
    return hID if int(hID) > 0 else 0


async def user_id_search_from_db(table_name_db, user_phone): #Поиск пользователя в БД
    # cursor = db.cursor()
    cur.execute(f"SELECT user_name "
                f"FROM {table_name_db} JOIN ID ON {table_name_db}.ID = ID.ID "
                f"WHERE user_phone = {user_phone}")
    result = cur.fetchone()
    # await result[0]
    return result[0] if result else None

async def list_table_from_db(table_name_db): #Возвращение списка ссылок для досуга
    df = pd.read_sql(f"SELECT * "
                     f"FROM {table_name_db}",
                     sq.connect('appointment.db')).values
    # txt = ""
    # for i in range(0, len(df)):
    #     txt+= df[i][0] + " " + df[i][1] + "\n"
    # return txt
    return df

# async def all_table_from_db(table_name_db): #Чтение всей таблицы во фрейм
#     df = pd.read_sql(f"SELECT * "
#                      f"FROM {table_name_db} AS A JOIN ID AS B ON A.ID = B.ID "
#                      f"JOIN IDTM AS C ON A.IDTM = C.IDTM "
#                      f"JOIN IDTD AS D ON A.IDTD = D.IDTD ",
#                      sq.connect('appointment.db'))
#     return df

async def CBA_table_from_db(table_name_db): #Чтение всей таблицы во фрейм
    df = pd.read_sql(f"SELECT * "
                     f"FROM {table_name_db} ",
                     sq.connect('appointment.db'))
    return df

# async def parametr_search_from_db(parametr, table_name_db, user_ID): #Поиск любого параметра
#     # cursor = db.cursor()
#     cur.execute(f"SELECT {parametr} "
#                 f"FROM {table_name_db} AS A "
#                 f"JOIN ID AS B ON A.ID = B.ID "
#                 f"JOIN IDTM AS C ON A.IDTM = C.IDTM "
#                 f"JOIN IDTD AS D ON A.IDTD = D.IDTD "
#                 f"WHERE A.user_ID = {user_ID}")
#     result = cur.fetchone()
#     # await result[0]
#     return result[0] if result else None

async def search_from_db(parametr, table_name_db, user_ID): #Поиск любого параметра по таблице CBAppointment
    # cursor = db.cursor()
    cur.execute(f"SELECT {parametr} "
                f"FROM {table_name_db} " 
                f"WHERE user_ID = {user_ID} ")
    result = cur.fetchone()
    # print(cur.execute)
    # await result[0]
    return result[0] if result else None

async def User_search_from_db(parametr, table_name_db, user_ID): #Поиск по таблице ID
    # cursor = db.cursor()
    cur.execute(f"SELECT {parametr}"
                f"FROM {table_name_db} AS A "
                f"JOIN ID AS B ON A.ID = B.ID "
                f"WHERE A.user_ID = {user_ID} ")
    result = cur.fetchone()
    # await result[0]
    return result[0] if result else None
async def meeting_search_from_db(parametr, table_name_db, user_ID): #Поиск по таблице meeting
    # cursor = db.cursor()
    cur.execute(f"SELECT {parametr} "
                f"FROM {table_name_db} AS A "
                f"JOIN IDTM AS C ON A.IDTM = C.IDTM "
                f"WHERE A.user_ID = {user_ID}")
    result = cur.fetchone()
    # await result[0]
    return result[0] if result else None

async def driver_search_from_db(parametr, table_name_db, user_ID): #Поиск по таблице driver
    # cursor = db.cursor()
    cur.execute(f"SELECT {parametr} "
                f"FROM {table_name_db} AS A "
                f"JOIN IDTD AS D ON A.IDTD = D.IDTD "
                f"WHERE A.user_ID = {user_ID}")
    result = cur.fetchone()
    # await result[0]
    return result[0] if result else None

# async def DB_replace_from_db(table_name, sheet_name): #Перезапись БД из гугл-таблицы
#     data = await read_table_google_sheets(table_name, sheet_name)
#     data.to_sql('CBAppointment', sq.connect('appointment.db'), if_exists='replace', index=False)

