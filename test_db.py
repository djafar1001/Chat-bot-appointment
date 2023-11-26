from loguru import logger
import sqlite3 as sq

global db, cur
db = sq.connect('appointment.db')
cur = db.cursor()
logger.info(f'{db= }\n{cur= }')
table_name = 'CBAppointment'
sheet_name = "DB"
cur.execute(f"SELECT * FROM sqlite_master WHERE type='table'")
result = cur.fetchall()

logger.debug(f'{result= }')

"""
Формат кортежа полей таблицы sqlite_master
('table',                       -  тип объекта                   - type
'Links',                        -  имя объекта                   - name
'Links',                        -  имя таблицы или представления 
                                   связанного с объектом         - tbl_name
7,                              -  номер корневой страницы       - rootpage
'CREATE TABLE "Links" (\n       -  SQL-запрос нормализованный    - sql
            "name_link" TEXT,\n
            "link" TEXT\n)')
"""
