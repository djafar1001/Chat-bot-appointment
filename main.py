import pandas as pd
import numpy as np
import sqlite3 as sq
import os
#  import pathlib
#  import datetime

from aiogram import Bot, Dispatcher, executor, types
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from SQL import db_start, user_id_from_db, user_id_search_from_db, list_table_from_db, help_from_db, \
    table_help_insert_from_db, CBA_table_from_db, meeting_search_from_db, driver_search_from_db, User_search_from_db, \
    search_from_db
from quickstart import create_writer_google_sheets, read_table_google_sheets, update_table_google_sheets, \
    create_table_google_sheets

#  Для логирования и отладки
from loguru import logger

debug_test = 1  # !!!Параметр работы в режиме отладки
# 1 - отладка кода, 0 - рабочий режим

if debug_test:
    from setings_test import TESTING_BOT
    botMes = Bot(TESTING_BOT['token'])
else:
    botMes = Bot(open(os.path.abspath('token.txt')).read())
bot = Dispatcher(botMes)

table_SH_name = 'Бот: встреча'
sheet_name = 'DB'
table_name_db = 'CBAppointment'
table_link = "Links"
tab_mes = 'Message'


async def on_startup(bot: Dispatcher):
    await db_start(table_name_db, sheet_name)


#
@bot.message_handler(commands=['start'])  # Начинаем работу
async def start(message: types.message):
    # global count
    # global page
    table_message = await read_table_google_sheets(table_SH_name, tab_mes)
    text = table_message['message'][0]
    request_contact_button = KeyboardButton(text="Отправить контакт", request_contact=True)
    reply_markup = ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
    reply_markup.add(request_contact_button)
    await message.reply(text=text,
                        reply_markup=reply_markup)  # Выводим сопутствующее сообщение


@bot.message_handler(content_types=types.ContentType.DOCUMENT)  # Перезагрузка таблиц ПД
async def process_document(message: types.Message):
    document = message.document
    if ".xlsx" in document.file_name:
        docName = document.file_name.partition('.')[0]
        try:
            os.remove(os.path.abspath(docName + '.xlsx'))
        except:
            pass
        await document.download(destination_file=f'{document.file_name}')
        table_replace = pd.read_excel(os.path.abspath(document.file_name))
        table_replace.to_sql(docName, sq.connect('appointment.db'), if_exists='replace', index=False)
        table_message = await read_table_google_sheets(table_SH_name, tab_mes)
        text = table_message['message'][1]
        await botMes.send_message(text=f"{text.format(docName)}", chat_id=message.chat.id)
    else:
        table_message = await read_table_google_sheets(table_SH_name, tab_mes)
        text = table_message['message'][2]
        await botMes.send_message(text=text, chat_id=message.chat.id)


# #
#

@bot.message_handler(content_types='text')  # Обработка всех текстовых сообщений
async def handle_message(message: types.Message):
    # await botMes.send_message(text='В связи с окончанием мероприятия чат-бот больше не отвечает на вопросы. Приносим свои извинения!', chat_id=message.chat.id)
    link_table_read = pd.DataFrame(await list_table_from_db(table_link))

    def link_hide(link_table_read):  # Заготовка списка ссылок для досуга
        txt = ""
        for i in range(0, len(link_table_read)):
            txt += f'<a href="{link_table_read[1][i]}">{link_table_read[0][i]} </a> \n'
        return txt

    if message.text == 'Информация о прибытии':  # Реакция на кнопку Информация о прибытии
        # Если информации нет для человека
        if await meeting_search_from_db("phone_meeting", table_name_db, message.chat.id) is None or len(
                str(await meeting_search_from_db("phone_meeting", table_name_db, message.chat.id))) < 3:
            table_message = await read_table_google_sheets(table_SH_name, tab_mes)
            text = table_message['message'][3]
            await botMes.send_message(text=f'{text}', chat_id=message.chat.id, parse_mode=types.ParseMode.HTML)
        else:  # Если информация есть для человека
            table_message = await read_table_google_sheets(table_SH_name, tab_mes)
            text = table_message['message'][4]
            phoneMeeting = "+" + str(int(await meeting_search_from_db("phone_meeting", table_name_db, message.chat.id)))
            text = text.format(await search_from_db("user_name", table_name_db, message.chat.id),
                               await search_from_db("place_arrival", table_name_db, message.chat.id),
                               await search_from_db("date_arrival", table_name_db, message.chat.id),
                               await search_from_db("time_arrival", table_name_db, message.chat.id),
                               await meeting_search_from_db("name_meeting", table_name_db, message.chat.id),
                               f'<a href="{phoneMeeting}">{phoneMeeting}</a>',
                               await search_from_db("arrival_flight_number", table_name_db, message.chat.id))
            await botMes.send_message(text=f'{text}', chat_id=message.chat.id, parse_mode=types.ParseMode.HTML)

    elif message.text == 'Информация о гостинице':  # Реакция на кнопку Информация о гостинице
        if await User_search_from_db("hotel_name", table_name_db, message.chat.id) is None or len(
                str(User_search_from_db("hotel_name", table_name_db, message.chat.id))) < 3:
            # Если информации нет для человека
            table_message = await read_table_google_sheets(table_SH_name, tab_mes)
            text = table_message['message'][12]
            await botMes.send_message(text=text.format(f'<a href="@Moiseeva_Ekaterina">@Moiseeva_Ekaterina</a>'),
                                      chat_id=message.chat.id, parse_mode=types.ParseMode.HTML)
        else:  # Если информация есть для человека
            table_message = await read_table_google_sheets(table_SH_name, tab_mes)
            text = table_message['message'][5]
            await botMes.send_message(
                text=text.format(await search_from_db("user_name", table_name_db, message.chat.id),
                                 await search_from_db("hotel_name", table_name_db, message.chat.id),
                                 await search_from_db("hotel_address", table_name_db, message.chat.id),
                                 await search_from_db("hotel_website", table_name_db, message.chat.id)),
                chat_id=message.chat.id)

    elif message.text == 'Информация об отъезде':  # Реакция на кнопку Информация об отъезде
        if await driver_search_from_db("driver_phone", table_name_db, message.chat.id) is None or len(
                str(await driver_search_from_db("driver_phone", table_name_db, message.chat.id))) < 3:
            # Если информации нет для человека
            table_message = await read_table_google_sheets(table_SH_name, tab_mes)
            text = table_message['message'][6]
            await botMes.send_message(text=text.format(f'<a href="@Moiseeva_Ekaterina">@Moiseeva_Ekaterina</a>'),
                                      chat_id=message.chat.id, parse_mode=types.ParseMode.HTML)
        else:  # Если информация есть для человека
            driverPhone = "+" + str(int(await driver_search_from_db("driver_phone", table_name_db, message.chat.id)))
            table_message = await read_table_google_sheets(table_SH_name, tab_mes)
            text = table_message['message'][7]
            await botMes.send_message(
                text=text.format(await search_from_db("user_name", table_name_db, message.chat.id),
                                 await search_from_db("date_departure", table_name_db, message.chat.id),
                                 await search_from_db("time_departure", table_name_db, message.chat.id),
                                 await driver_search_from_db("driver_name", table_name_db, message.chat.id),
                                 f'<a href="{driverPhone}">{driverPhone}</a>',
                                 await search_from_db("departure_flight_number", table_name_db, message.chat.id)),
                chat_id=message.chat.id, parse_mode=types.ParseMode.HTML)


    elif message.text == 'Досуг':  # Реакция на кнопку Досуг
        if await search_from_db("user_name", table_name_db, message.chat.id) is None or len(
                str(await search_from_db("user_name", table_name_db, message.chat.id))) < 3:
            # Если информации нет для человека
            table_message = await read_table_google_sheets(table_SH_name, tab_mes)
            text = table_message['message'][12]
            await botMes.send_message(text=text.format(f'<a href="@Moiseeva_Ekaterina">@Moiseeva_Ekaterina</a>'),
                                      chat_id=message.chat.id, parse_mode=types.ParseMode.HTML)
        else:  # Если информация есть для человека
            table_message = await read_table_google_sheets(table_SH_name, tab_mes)
            logger.debug(f'{table_message}')
            text = table_message['message'][8]
            await botMes.send_message(
                text=text.format(await search_from_db("user_name", table_name_db, message.chat.id),
                                 link_hide(link_table_read)), chat_id=message.chat.id, parse_mode=types.ParseMode.HTML)

    elif message.text == 'Помощь':  # Реакция на кнопку Помощь
        await help_from_db(table_name_db=table_name_db, help_request='1', user_id=message.chat.id)
        table_message = await read_table_google_sheets(table_SH_name, tab_mes)
        text = table_message['message'][9]
        await botMes.send_message(text=text, chat_id=message.chat.id)

    # elif message.text == 'Брошюра': #Реакция на кнопку Брошюра (временно исключена)
    #     table_message = await read_table_google_sheets(table_SH_name, tab_mes)
    #     text = table_message['message'][10]
    #     await botMes.send_message(text=text, chat_id=message.chat.id)

    elif message.text == 'Информационный канал':  # Реакция на кнопку Информационный канал
        table_message = await read_table_google_sheets(table_SH_name, tab_mes)
        text = table_message['message'][11]
        await botMes.send_message(text=text.format('<a href="https://t.me/+qkAdTCzGt89jYzgy">ссылка</a>'),
                                  chat_id=message.chat.id, parse_mode=types.ParseMode.HTML)


    elif await search_from_db('help_request', table_name_db, message.chat.id) == '1':  # Пересылка запроса о помощи
        await help_from_db(table_name_db, '0', message.chat.id)
        hID = await table_help_insert_from_db(message.chat.id, message.text)
        if await User_search_from_db("user_phone", table_name_db, message.chat.id) is None or len(
                str(await User_search_from_db("user_phone", table_name_db, message.chat.id))) < 3:
            # Если человека нет в базе
            table_message = await read_table_google_sheets(table_SH_name, tab_mes)
            text = table_message['message'][12]
            await botMes.send_message(text=text.format('<a href="@Moiseeva_Ekaterina">@Moiseeva_Ekaterina</a>'),
                                      chat_id=message.chat.id, parse_mode=types.ParseMode.HTML)
        else:  # Если человек есть в базе
            id_chat_DDKP = open(os.path.abspath('id.txt')).read()
            phoneMeeting = "+" + str(int(await User_search_from_db("user_phone", table_name_db, message.chat.id)))
            buttons = [['Информация о прибытии', 'Информация о гостинице'], ['Информация об отъезде', 'Досуг'],
                       ['Помощь', 'Информационный канал']]
            markup = ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=False)
            table_message = await read_table_google_sheets(table_SH_name, tab_mes)
            text = table_message['message'][13]
            await botMes.send_message(text=text, chat_id=message.chat.id, reply_markup=markup)
            text = table_message['message'][14]
            await botMes.send_message(text=text.format(hID,
                                                       await search_from_db("FIO", table_name_db, message.chat.id),
                                                       f'<a href="{phoneMeeting}">{phoneMeeting}</a>', message.text),
                                      chat_id=id_chat_DDKP, parse_mode=types.ParseMode.HTML)

    elif message.text == 'Обновить основную таблицу':  # Реакция на кнопку Обновить основную таблицу (иногда матерится)
        if await search_from_db("user_role", table_name_db, message.chat.id) == 'Админ':  # Если это админ
            db_table = await CBA_table_from_db(table_name_db)
            sh_table = await read_table_google_sheets(table_SH_name, sheet_name)
            db_table = db_table.sort_values(by='ID')
            sh_table = sh_table.sort_values(by='ID')
            out_table = sh_table.copy()
            out_table['user_ID'] = ""
            out_table['help_request'] = ""
            db_table = db_table.astype({'ID': object})

            for i in sh_table['ID'].values:
                try:
                    user_ID = db_table[db_table['ID'] == i]['user_ID'].values[0]
                    out_table['user_ID'][out_table[out_table['ID'] == i].index[0]] = db_table['user_ID'][
                        db_table[db_table['user_ID'] == user_ID].index[0]]
                    # print(out_table['user_ID'][out_table[out_table['ID'] == i].index[0]])
                except:
                    out_table['user_ID'][out_table[out_table['ID'] == i].index[0]] = np.nan

            for i in sh_table['ID'].values:
                try:
                    user_ID = db_table[db_table['ID'] == i]['help_request'].values[0]
                    out_table['help_request'][out_table[out_table['ID'] == i].index[0]] = db_table['help_request'][
                        db_table[db_table['help_request'] == user_ID].index[0]]
                except:
                    out_table['help_request'][out_table[out_table['ID'] == i].index[0]] = np.nan

            out_table.to_sql('CBAppointment', sq.connect('appointment.db'), if_exists='replace', index=False)
            sh_table_link = await read_table_google_sheets(table_SH_name, table_link)
            sh_table_link.to_sql('Links', sq.connect('appointment.db'), if_exists='replace', index=False)
            table_message = await read_table_google_sheets(table_SH_name, tab_mes)
            text = table_message['message'][15]
            await botMes.send_message(text=text, chat_id=message.chat.id)
        else:  # Если это не админ
            await botMes.send_message(
                text='Пользователь, не являющийся организатором (администратором) не может выполнить это действие!',
                chat_id=message.chat.id)

    elif message.text == 'Обновить таблицу ПД':  # Реакция на кнопку Обновить таблицу ПД
        if await search_from_db("user_role", table_name_db, message.chat.id) == 'Админ':  # Если это админ
            table_message = await read_table_google_sheets(table_SH_name, tab_mes)
            text = table_message['message'][16]
            await botMes.send_message(text=text, chat_id=message.chat.id)
        else:  # Если это не админ
            await botMes.send_message(
                text='Пользователь, не являющийся организатором (администратором) не может выполнить это действие!',
                chat_id=message.chat.id)



    elif message.text == 'Получить доступ к таблице':  # Реакция на кнопку Получить доступ к таблице
        if await search_from_db("user_role", table_name_db, message.chat.id) == 'Админ':  # Если это админ
            table_message = await read_table_google_sheets(table_SH_name, tab_mes)
            text = table_message['message'][17]
            await botMes.send_message(text=text, chat_id=message.chat.id)
        else:  # Если это не админ
            await botMes.send_message(
                text='Пользователь, не являющийся организатором (администратором) не может выполнить это действие!',
                chat_id=message.chat.id)

    elif '@gmail.com' in message.text and await search_from_db("user_role", table_name_db,
                                                               message.chat.id) == 'Админ':  # Если это админ и верно введена почта, то продолжить
        try:
            await create_writer_google_sheets(table_SH_name, message.text)
            table_message = await read_table_google_sheets(table_SH_name, tab_mes)
            text = table_message['message'][18]
            await botMes.send_message(text=text, chat_id=message.chat.id)
        except:
            table_message = await read_table_google_sheets(table_SH_name, tab_mes)
            text = table_message['message'][19]
            await botMes.send_message(text=text,
                                      chat_id=message.chat.id)


@bot.message_handler(content_types=types.ContentType.CONTACT)  # Регистрация пользователя
async def contacts(message: types.Message, table_name_db=table_name_db):
    # Ищем человека в БД
    calB = await user_id_search_from_db(table_name_db, message.contact.phone_number)
    if calB is None:  # Если пользователя нет в БД
        table_message = await read_table_google_sheets(table_SH_name, tab_mes)
        text = table_message['message'][20]
        await message.reply(text=text)
        text = table_message['message'][21]
        id_chat_DDKP = open(os.path.abspath('id.txt')).read()
        await botMes.send_message(text=text.format(message.contact.full_name,
                                                   f'<a href="+{message.contact.phone_number}">+{message.contact.phone_number}</a>')
                                  , chat_id=id_chat_DDKP,
                                  parse_mode=types.ParseMode.HTML)


    else:  # Если пользователь есть в БД
        await user_id_from_db(table_name_db=table_name_db, user_id=message.chat.id,
                              user_phone=message.contact.phone_number)
        # Если у пользователя роль Гость
        if await search_from_db("user_role", table_name_db, message.chat.id) == "Гость":
            table_message = await read_table_google_sheets(table_SH_name, tab_mes)
            text = table_message['message'][22]
            await message.reply(text=text)
            buttons = [['Информация о прибытии', 'Информация о гостинице'], ['Информация об отъезде', 'Досуг'],
                       ['Помощь', 'Информационный канал']]
        else:  # Если у пользователя роль Админ
            table_message = await read_table_google_sheets(table_SH_name, tab_mes)
            text = table_message['message'][23]
            await message.reply(text=text.format(await search_from_db('user_name', table_name_db, message.chat.id)))
            buttons = [['Информация о прибытии', 'Информация о гостинице'], ['Информация об отъезде', 'Досуг'],
                       ['Обновить основную таблицу', 'Обновить таблицу ПД'], ['Получить доступ к таблице']]
        markup = ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=False)
        table_message = await read_table_google_sheets(table_SH_name, tab_mes)
        text = table_message['message'][24]
        await botMes.send_message(chat_id=message.chat.id, text=text, reply_markup=markup)


if __name__ == '__main__':
    # Бесконечно запускаем бот и игнорим ошибки
    while True:
        try:
            print('Started')
            executor.start_polling(bot, on_startup=on_startup)
        except:
            logger.exception('Произошла ошибка')

    #         pass
    # print('Started')
    # executor.start_polling(bot, on_startup=on_startup)
