from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
import json
from bson import json_util
from users.userschema import UserSchema

from utils.utils import db_add_notification,db_read_notification,send_to_dlq,db_get_notifications_by_user_id
from config.config import (
    KAFKA_PASSWORD,
    KAFKA_USER,
    KAFKA_URL,
    KAFKA_CONSUMER_CONF,
    KAFKA_PRODUCER_CONF,
)
from users.users import get_users_data
import re


async def get_or_create_topic(topic_name):
    """Получение или создание топика"""
    client = AIOKafkaAdminClient(
        bootstrap_servers=KAFKA_URL,
        sasl_plain_username=KAFKA_USER,
        sasl_plain_password=KAFKA_PASSWORD,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
    )
    await client.start()

    try:
        if topic_name in await client.list_topics():  # Проверка существует ли топик
            return topic_name
        await client.create_topics(
            [NewTopic(name=topic_name, replication_factor=3, num_partitions=1)]
        )
        return topic_name
    finally:
        await client.close()


async def add_response(body, error=None, *args, **kwargs):
    """Функция сохранения респонса в кафку"""
    data = {
        "request_id": str(body["request_id"]),
        "message": {
            "status": kwargs["status"],
            "error": error,
            "action": body["message"]["action"],
            "body": {},
        },
    }
    try:
        data["message"]["body"].update(
            kwargs["data"]
        )  # Добавление к респонсу информации о запросе
    except KeyError as ex:
        print(ex)

    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_URL, **KAFKA_PRODUCER_CONF)
    await producer.start()
    try:
        await get_or_create_topic("notification_responses")
        await producer.send_and_wait(
            "notification_responses", value=json_util.dumps(data).encode("utf-8")
        )
    finally:
        await producer.stop()


async def get_user_all_notifications(body):
    """Получение данных по user_id
    action: user_notifications
    body: {
        user_id:int,
        is_read:bool/None
    }

    """
    is_readed = None
    user_id = body["message"]["body"]["user_id"]

    try:
        is_readed = body["message"]["body"]["is_read"]
    except KeyError:
        print("Нет в словаре")

    if (
        type(is_readed) is bool or is_readed is None
    ):  # Проверка передается ли is_read в запросе. если передается что-то отличное от True/False/None - возврат fail
        notifications = await db_get_notifications_by_user_id(user_id, is_readed)
        await add_response(
            body=body,
            status="success",
            data={
                "notifications_quantity": len(notifications),
                "notifications": notifications,
            },
        )
    else:
        await add_response(body=body, status="fail")


async def add_notifications(body):
    """Создание уведомлений в монго и сохранение респонса в кафку
    action: add_notification
    """
    request_id = body["request_id"]

    message_template = body["message"]["body"]["message_template"]
    user_id = (
        re.search(r"\[(.*?)\]", message_template).group(1).split(":")[1]
    )  # Регулярка для отделения айдишника юзера

    user_info = await get_users_data(request_id=request_id, user_id=user_id)
    if isinstance(user_info, UserSchema):
        body["message"]["body"]["message_final"] = message_template.replace(
            f"[{re.search(r'\[(.*?)\]', message_template).group(1)}]",
            f"{user_info.last_name} {user_info.first_name}",
        )

        id = await db_add_notification(body)
        await add_response(body, status="success", data={"notification_id": id})
    else:
        error = user_info["error"]
        await add_response(body, status="fail", error=error)


async def read_notification(body):
    """Редактирование и сохранение реквеста в монго
    action: read_notification
    """
    notifications = await db_read_notification(body)
    if notifications:
        await add_response(
            body,
            status="success",
            data={"notification_id": body["message"]["body"]["notification_id"]},
        )
    else:
        
        await add_response(body, status="fail")


async def process_data(action, body=None):
    """Фильтр на команды"""
    actions = {
        "add_notification": add_notifications,
        "user_notifications": get_user_all_notifications,
        "read_notification": read_notification,
    }
    if action in actions:
        return await actions[action](body)
    else:
        await get_or_create_topic("dead_letter")
        await send_to_dlq(json.dumps(body).encode("utf-8"), error='wrong action')


async def consume_data():
    """Получение данных из кафки"""
    await get_or_create_topic("notification_requests")  # Проверка существует ли топик
    consumer = AIOKafkaConsumer(
        "notification_requests",
        group_id="ms-notifications",
        bootstrap_servers=KAFKA_URL,
        **KAFKA_CONSUMER_CONF,
    )
    await consumer.start()

    try:
        async for msg in consumer:
            raw_data = msg.value.decode("utf-8")
            msg_data = json.loads(raw_data)
            if msg_data["message"]["action"] is None:
                await get_or_create_topic("dead_letter")
                print('Улетели отсюда')
                await send_to_dlq(msg_data)
            await process_data(action=msg_data["message"]["action"], body=msg_data)

    finally:
        await consumer.stop()



