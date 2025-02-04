from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
import asyncio
import json
import os
from utils import db_get_users, db_add_notification, db_read_notification, send_to_dlq


KAFKA_URL = os.getenv("KAFKA_URL")


async def add_response(body, *args, **kwargs):
    """Функция сохранения респонса в кафку"""
    data = {
        "request_id": body["request_id"],
        "message": {
            "action": body["message"]["action"],
            "body": {"message": "success"},
        },
    }

    data["message"]["body"].update(
        kwargs["data"]
    )  # Добавление к респонсу информации о запросе

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_URL)
    await producer.start()

    await producer.send_and_wait(
        "notification_responses", value=json.dumps(data).encode("utf-8")
    )
    await producer.stop()


async def get_users(message):
    """Получение всех пользователей (вывод просто принтом стоит)"""
    users = await db_get_users(message["message"]["body"])
    await add_response(message, data={"users": users})


async def add_notifications(body):
    """Создание уведомлений в монго и сохранение респонса в кафку"""
    id = await db_add_notification(body)
    await add_response(body, data=id)


async def edit_document(body):
    """Редактирование и сохранение реквеста в монго"""
    notification = await db_read_notification(body)
    await add_response(
        body, data={"notification_id": body["message"]["body"]["notification_id"]}
    )


async def process_data(action, body=None):
    """Фильтр на команды"""
    actions = {
        "add_notification": add_notifications,
        "get_users": get_users,
        "read_notification": edit_document,
    }
    if action in actions:
        return await actions[action](body) if actions[action] else None
    else:
        await send_to_dlq(json.dumps(body).encode("utf-8"))


async def consume_data():
    """Получение данных из кафки"""
    consumer = AIOKafkaConsumer(
        "notification_requests",
        group_id="data_consumber",
        bootstrap_servers=KAFKA_URL,
    )
    await consumer.start()
    
    try:
        async for msg in consumer:
            raw_data = msg.value.decode("utf-8")
            msg_data = json.loads(raw_data)
            if msg_data["message"]["action"] is None    :
                await send_to_dlq(msg)
            await process_data(action=msg_data["message"]["action"], body=msg_data)

    finally:
        await consumer.stop()


async def main():
    try:
        asyncio.create_task(await consume_data())
    except KafkaConnectionError as ex:
        print(ex)


if __name__ == "__main__":
    asyncio.run(main())
