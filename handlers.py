from aiogram import Router
from aiogram.types import Message
from json import JSONDecodeError

from utils import combine_data


router = Router()


@router.message()
async def message_handler(msg: Message) -> None:
    result = await combine_data(msg.text)
    if 'error' in result:
        await msg.answer(str(result['error']))
        return
    await msg.answer(str(result))
