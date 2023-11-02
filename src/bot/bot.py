import json
from datetime import datetime

from aiogram import Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message
from aiogram.utils.markdown import hbold

from src.logger.logger import tb_logger


def setup_bot_handlers(dp: Dispatcher, database):

    @dp.message(CommandStart())
    async def command_start_handler(message: Message) -> None:
        await message.answer(f"Hi, {hbold(message.from_user.username)}")

    @dp.message()
    async def fetch_data(message: Message) -> None:
        error_message = (
            'Невалидный запрос. Пример запроса:\n'
            '{"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}'
        )
        try:
            query = json.loads(message.text)
            # Проверим, что формат сообщения соответствует ожиданиям.
            if not all(key in query for key in ["dt_from", "dt_upto", "group_type"]):
                await message.answer(error_message)
                return

            # Проверим, что даты корректны.
            date_format = "%Y-%m-%dT%H:%M:%S"
            for date_key in ["dt_from", "dt_upto"]:
                try:
                    datetime.strptime(query[date_key], date_format)
                except ValueError:
                    await message.answer(error_message)
                    return

            result = await database.get_data_from_db(query)
            await message.answer(json.dumps(result, ensure_ascii=False))

        except json.JSONDecodeError:
            await message.answer(error_message)
        except Exception as e:
            tb_logger.log_info(f"Error in fetch_data: {str(e)}")
            await message.answer("Произошла ошибка при выполнении запроса.")
