from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command
import os
import asyncio
from aiokafka import AIOKafkaConsumer
import json

# BOT_TOKEN = os.getenv('BOT_TOKEN')
BOT_TOKEN = "ENTER TOKEN"
KAFKA_URL = os.getenv("KAFKA_URL")


bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
chats = [403432624]


@dp.message(Command("id"))
async def get_chat_id(message: Message):
    chat_id = message.chat.id
    chats.append(chat_id)
    await message.answer(f"ID = {chat_id}")


async def consume_book_data():
    consumer = AIOKafkaConsumer(
        "book_data",
        group_id="bot_consumer",
        bootstrap_servers=KAFKA_URL,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )
    await consumer.start()

    try:
        async for msg in consumer:
            msg_data = msg.value

            for chat in chats:
                await bot.send_message(
                    chat_id=chat, text=f"\n{msg_data}\n", parse_mode="Markdown"
                )
    finally:
        await consumer.stop()


async def main():
    asyncio.create_task(consume_book_data())

    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    asyncio.run(main())
