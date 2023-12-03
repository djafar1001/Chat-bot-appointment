import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
import df2gspread as d2g
import numpy as np
import pandas as pd

from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
# 'chat.bot.kpi@gmail.com'

async def create_table_google_sheets(table_name): #Создание новой таблицы
    # Подсоединение к Google Таблицам
    scope = ['https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive"]

#    credentials = ServiceAccountCredentials.from_json_keyfile_name("cbappoitment-5965445a13a2.json", scope)
    credentials = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(credentials)
    client.create(table_name)


async def create_writer_google_sheets(table_name, email): #Добавление нового редактора в таблицу
    # Подсоединение к Google Таблицам
    scope = ['https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive"]

#    credentials = ServiceAccountCredentials.from_json_keyfile_name("cbappoitment-5965445a13a2.json", scope)
    credentials = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(credentials)
    client.open(table_name).share(email, perm_type='user', role='writer')


async def read_table_google_sheets(table_name, sheet_name): #Считать таблицу во фрейм (с отступами по шапке)
    # Подсоединение к Google Таблицам
    # print(1)
    scope = ['https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive"]

#    credentials = ServiceAccountCredentials.from_json_keyfile_name("cbappoitment-5965445a13a2.json", scope)
    credentials = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(credentials)
    sheet = client.open(table_name).worksheet(sheet_name)
    # print(sheet.get_all_values())
    data = pd.DataFrame(sheet.get_all_values())
    data.columns = np.array(data.iloc[1])
    data = data.reindex(data.index.drop(0))
    data = data.reindex(data.index.drop(1))
    data.reset_index(drop=True, inplace=True)
    # print(data)
    return data


async def update_table_google_sheets(table_name, sheet_name, out_table): #Обновить гугл-таблицу
    # Подсоединение к Google Таблицам
    scope = ['https://www.googleapis.com/auth/spreadsheets',
                 "https://www.googleapis.com/auth/drive"]

#    credentials = ServiceAccountCredentials.from_json_keyfile_name("cbappoitment-5965445a13a2.json", scope)
    credentials = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(credentials)
    sheet = client.open(table_name).worksheet(sheet_name)

    set_with_dataframe(sheet, out_table, row=2, include_column_header=True)


# async def create_reply_menu(role):
#     if role:
#         buttons = [['Информация о прибытии', 'Информация о гостинице'], ['Информация об отъезде', 'Досуг'],
#                    ['Помощь', 'Информационный канал'], ['Дополнительные материалы']]
#     else:
#
#     markup = ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=False)
#
#     return markup
#     pass
