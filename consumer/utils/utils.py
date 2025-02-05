from db.db import app
from datetime import datetime
from bson import ObjectId
from aiokafka import AIOKafkaProducer
import os
import json 

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
    """Функция для добавления уведомления в монго 

    Args:
        message (_type_): Получаем из notifications.add_notifications

    Returns:
        str: Возврат ID'шника добавленного уведомления 
    """
    
    body = message["message"]["body"]
    data = {
        "user_id": str(body["user_id"]),
        "senders": [
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
    """Запись новой информации в уведомления

    Args:
        message (_type_): Получаем из notifications.read_notification

    Returns:
        _type_: _description_
    """
    try:
        body = message["message"]["body"]
        update_data = {"$set": {"is_read": True, "read_at": datetime.now()}}
        notification = app.notifications.update_one(
            filter={"_id": ObjectId(body["notification_id"])}, update=update_data
        )

        return str(notification)
    except ConnectionError:
        return None


async def send_to_dlq(message, error=None):
    """Отправка сообщения в мертвую очередь
    Если передать ошибку выведет и ошибку, иначе None 
    Args:
        message (_type_): Само сообщение, которое не удалосб обработать
        error (_type_, optional): Сообщение об ошибке
    """
    
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_URL, **KAFKA_PRODUCER_CONF)
    raw_data = message.decode('utf-8')
    msg_data = json.loads(raw_data)
    
    await producer.start()
    
    data = {
	"request_id": f"{msg_data['request_id']}",
	"message": {
		"status": "fail",
		"error": error if error else None,
		"action": f"{msg_data['message']['action']}",
		"body": {}
                }
            }

    try:
        await producer.send_and_wait("dead_letter", value=message)
        await producer.send_and_wait('notification_responses', value=json.dumps(data).encode('utf-8'))
    finally:
        await producer.stop()


async def db_get_notifications_by_user_id(user_id: str, is_read):
    """Получение всех уведомлений для юзера 
        action:user_notifications

    Args:
        user_id (str): Айдишник искомого юзера 
        is_read (bool): True/False/None Если None - возврат всех уведомлений

    Returns:
        notifications
    """
    try:
        if is_read is not None:
            query = app.notifications.find({"user_id": user_id, "is_read": is_read})
        else:
            query = app.notifications.find({"user_id": user_id})

        users = list(query)
        return users
    except ConnectionError:
        return None
