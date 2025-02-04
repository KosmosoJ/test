from db import app
from datetime import datetime
from bson import ObjectId
from aiokafka import AIOKafkaProducer
import os
from pymongo.errors import ConnectionFailure

KAFKA_URL = os.getenv("KAFKA_URL")
KAFKA_USER = os.getenv("KAFKA_USER")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")

KAFKA_PRODUCER_CONF = {
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "sasl_plain_username": KAFKA_USER,
    "sasl_plain_password": KAFKA_PASSWORD,
}



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
    return str(users)


async def db_read_notification(message):
    """Запись новой информации в уведомления"""
    try:
        body = message["message"]["body"]
        update_data = {"$set": {"is_read": True, "read_at": datetime.now()}}
        notification = app.notifications.update_one(
            filter={"_id": ObjectId(body["notification_id"])}, update=update_data
        )

        return str(notification)
    except ConnectionError:
        return None 


async def send_to_dlq(message):
    """Отправка сообщения в мертвую очередь"""
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_URL, **KAFKA_PRODUCER_CONF)
    await producer.start()

    try:
        await producer.send_and_wait("dead_letter", value=message)
    finally:
        await producer.stop()


async def db_get_notifications_by_user_id(user_id:str, is_read):
    try:
        if is_read is not None:
            query = app.notifications.find({'user_id':user_id, 'is_read':is_read})
        else:
            query = app.notifications.find({'user_id':user_id})
        
        users = list(query)
        return users 
    except ConnectionError:
        return None 
        