import asyncio
import os

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode

from src.logger.logger import tb_logger
from src.mongo.mongo import Mongo
from src.bot.bot import setup_bot_handlers

TOKEN = os.environ.get("TOKEN")

MONGO_USERNAME = os.environ.get("MONGO_USERNAME")
MONGO_PASSWORD = os.environ.get("MONGO_PASSWORD")
MONGO_CLUSTER = os.environ.get("MONGO_CLUSTER")
DATABASE_NAME = os.environ.get("DATABASE_NAME")
COLLECTION_NAME = os.environ.get("COLLECTION_NAME")
PATH_TO_BSON = os.environ.get("PATH_TO_BSON")


update_db = os.environ.get("UPDATE_DB")

if None in [MONGO_USERNAME, MONGO_PASSWORD, MONGO_CLUSTER, DATABASE_NAME, COLLECTION_NAME]:
    raise ValueError("Отсутствуют данные для подключения к базе данных.")
if TOKEN is None:
    raise Exception("where is token?")


async def main():
    db = Mongo(MONGO_USERNAME, MONGO_PASSWORD, MONGO_CLUSTER, DATABASE_NAME, COLLECTION_NAME)
    if update_db:
        tb_logger.log_info("Импортируем .bson в базу данных.")
        await db.import_bson_to_db(PATH_TO_BSON)
        tb_logger.log_info("Данные импортированы.")
    bot = Bot(token=TOKEN, parse_mode=ParseMode.HTML)
    dp = Dispatcher()
    setup_bot_handlers(dp, db)
    tb_logger.log_info("Bot has been initialized.")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
