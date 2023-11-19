import asyncio
from quickstart import create_writer_google_sheets, create_table_google_sheets


table_SH_name = 'Бот: встреча'
sheet_name = 'DB'
table_name_db = 'CBAppointment'
table_link = "Links"
tab_mes = 'Message'
my_mail = 'alexis120270@gmail.com'

#
async def main():
    await create_table_google_sheets(table_SH_name)
    await create_writer_google_sheets(table_SH_name, my_mail)

asyncio.run(main())
#asyncio.run(create_writer_google_sheets(table_SH_name, my_mail))

