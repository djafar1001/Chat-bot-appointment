import time
import os
import asyncio
import pandas as pd
#Скрипт для выгрузки списка зарегистрированных в боте пользователей
from SQL import CBA_table_from_db

async def my_function():
    df = await CBA_table_from_db('CBAppointment')
    df = df[df['user_ID'].notnull()]
    df1 = pd.DataFrame()
    df1['ID'] = df['ID']
    df1['FIO'] = df['FIO']
    df1.to_excel(os.path.abspath('output.xlsx'), index=False)

async def main():
    while True:
        current_time = time.strftime("%H:%M:%S", time.localtime())
        if current_time == "16:12:00" or current_time == "20:00:00":
            await my_function()
        await asyncio.sleep(1)

asyncio.run(main())