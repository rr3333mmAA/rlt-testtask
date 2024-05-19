import asyncio
import logging
import sys
from os import getenv

from aiogram import Bot, Dispatcher, html
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import Message

from functions import parse_json_text, aggregate_salary_data

TOKEN = getenv('BOT_TOKEN')
dp = Dispatcher()


@dp.message(CommandStart())
async def command_start_handler(message: Message) -> None:
    """
    Command /start handler
    :param message: Message
    :return: None
    """
    await message.answer(f"Hello, {html.bold(message.from_user.full_name)}!")


@dp.message()
async def parse_data(message: Message) -> None:
    """
    Parse data
    :param message: Message
    :return: None
    """
    dt_from, dt_upto, group_type = await parse_json_text(message.text)
    if dt_from is not None:
        data = await aggregate_salary_data('sample_collection.bson', dt_from, dt_upto, group_type)
        if data is not None:
            text = f'{data}'
            await message.answer(text)
            return

    text = ('Невалидный запос. Пример запроса:\n\n'
            '{"dt_from": "2022-09-01T00:00:00", "dt_upto": "2022-12-31T23:59:00", "group_type": "month"}')
    await message.answer(text)


async def main() -> None:
    """
    Main function
    :return: None
    """
    bot = Bot(token=TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    asyncio.run(main())
