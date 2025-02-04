from db import app
from datetime import datetime
from bson import ObjectId
from aiokafka import AIOKafkaProducer
import os

KAFKA_URL = os.getenv("KAFKA_URL")


async def db_get_users(body=None):
    """Получение всех юзеров из монго"""
    raw_users = app.users.find()
    users = list(raw_users)

    return users


async def db_add_notification(message):
    """Добавление нового уведомления в монго"""
    body = message["message"]["body"]
    data = {
        "user_id": str(body["user_id"]),
        "senders": [
            {"type": "user", "id": "6789"},
            {"type": "application", "id": "app_456"},
        ],
        "title": body["title"],
        "message_template": body["message_template"],
        "message_final": body["message_final"],
        "created_at": datetime.now(),
        "is_read": False,
        "read_at": None,
        "metadata": {
            "priority": body["metadata"]["priority"],
            "type": body["metadata"]["type"],
        },
    }
    users = app.notifications.insert_one(data).inserted_id
    return users


async def db_read_notification(message):
    """Запись новой информации в уведомления"""
    body = message["message"]["body"]
    update_data = {"$set": {"is_read": True, "read_at": datetime.now()}}
    notification = app.notifications.update_one(
        filter={"_id": ObjectId(body["notification_id"])}, update=update_data
    )

    return str(notification)


async def send_to_dlq(message):
    """ Отправка сообщения в мертвую очередь """
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_URL)
    await producer.start()

    try:
        await producer.send_and_wait("dead_letter",value=message)
    finally:
        await producer.stop()
