from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaConnectionError
import asyncio
import json
import os
from utils import db_get_users, db_add_notification, db_read_notification, send_to_dlq


KAFKA_URL = os.getenv("KAFKA_URL")
KAFKA_USER = os.getenv("KAFKA_USER")
KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")


KAFKA_PRODUCER_CONF = {
    "security_protocol": "SASL_PLAINTEXT",
    "sasl_mechanism": "PLAIN",
    "sasl_plain_username": KAFKA_USER,
    "sasl_plain_password": KAFKA_PASSWORD,
}

KAFKA_CONF = {
    "security_protocol": "SASL_PLAINTEXT",
    "enable_auto_commit": True,
    "auto_offset_reset": "earliest",
    "session_timeout_ms": 10000,
    "sasl_mechanism": "PLAIN",
    "sasl_plain_username": KAFKA_USER,
    "sasl_plain_password": KAFKA_PASSWORD,
}


async def get_kafka_producer():
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_URL, **KAFKA_PRODUCER_CONF)
    await producer.start()

    return producer


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


async def get_kafka_consumer():
    producer = AIOKafkaConsumer(
        bootstrap_servers=KAFKA_URL, group_id="ms-notifications", **KAFKA_CONF
    )
    await producer.start()
    return producer


async def add_response(body, *args, **kwargs):
    """Функция сохранения респонса в кафку"""
    data = {
        "request_id": str(body["request_id"]),
        "message": {
            "action": body["message"]["action"],
            "body": {"message": "success"},
        },
    }
    data["message"]["body"].update(
        kwargs["data"]
    )  # Добавление к респонсу информации о запросе
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_URL, **KAFKA_PRODUCER_CONF)
    await producer.start()
    try:
        await get_or_create_topic("notification_responses")
        await producer.send_and_wait(
            "notification_responses", value=json.dumps(data).encode("utf-8")
        )
    finally:
        await producer.stop()


async def get_users(message):
    """Получение всех пользователей (вывод просто принтом стоит)"""
    users = await db_get_users(message["message"]["body"])
    await add_response(message, data={"users": users})


async def add_notifications(body):
    """Создание уведомлений в монго и сохранение респонса в кафку"""
    id = await db_add_notification(body)
    await add_response(body, data={"request_id": id})


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
        return await actions[action](body)
    else:
        await get_or_create_topic("dead_letter")
        await send_to_dlq(json.dumps(body).encode("utf-8"))


async def consume_data():
    """Получение данных из кафки"""
    await get_or_create_topic("notification_requests")  # Проверка существует ли топик
    consumer = AIOKafkaConsumer(
        "notification_requests",
        group_id="ms-notifications",
        bootstrap_servers=KAFKA_URL,
        **KAFKA_CONF,
    )
    await consumer.start()

    try:
        async for msg in consumer:
            raw_data = msg.value.decode("utf-8")
            msg_data = json.loads(raw_data)
            if msg_data["message"]["action"] is None:
                await get_or_create_topic("dead_letter")
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
