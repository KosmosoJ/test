from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from config.config import KAFKA_CONSUMER_CONF, KAFKA_PRODUCER_CONF, KAFKA_URL
import json
from bson import json_util
import uuid
from .userschema import UserSchema


async def get_users_data(request_id, user_id):
    """Функция-сборщик

    Args:
        request_id : реквест от которого идет запрос на юзера
        user_id : искомый юзер, из notifications.add_notifcation

    Returns:
        UserSchema/dict{'error':error}
    """
    await produce_get_user_request(
        user_id=user_id, request_id=request_id
    )  # Запрос на юзера
    user_data = await consume_users_data(request_id)  # Получение юзера
    return user_data


async def process_user_data(user_info):
    """Метод для получения имени и фамилии из user_response

    Args:
        user_info : Респонс из user_response

    Returns:
        UserSchema
    """
    if (
        user_info["message"]["status"] == "success"
    ):  # Проверка успешно ли получили пользователя
        user = UserSchema(
            first_name=user_info["message"]["body"]["first_name"].strip(),
            last_name=user_info["message"]["body"]["last_name"].strip(),
        )
        return user
    else:
        return {"error": user_info["message"]["error"]}


async def consume_users_data(request_id):
    """Консюмер для user_response

    Args:
        request_id (_type_): request_id получаем из notifications.add_notifications

    Returns:
        UserSchema
    """
    consumer = AIOKafkaConsumer("user_responses", bootstrap_servers=KAFKA_URL, **KAFKA_CONSUMER_CONF)

    await consumer.start()
    try:
        async for msg in consumer:
            raw_data = msg.value.decode("utf-8")
            msg_data = json.loads(raw_data)
            if msg_data["request_id"] == request_id:
                print('Совпало')
                return await process_user_data(msg_data)
    finally:
        await consumer.stop()


async def produce_get_user_request(user_id, request_id):
    """Продюсер для user_request

    Args:
        user_id (_type_): Получаем из get_users_data
        request_id (_type_): Получаем из get_users_data
    """
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_URL, **KAFKA_PRODUCER_CONF)
    
    try:
        data = {
            "request_id": f"{str(request_id)}",
            "message": {"action": "read_user", "body": {"user_id": str(user_id.strip())}},
        }
    except TypeError as ex:
        print(type(request_id), type(user_id))
        print('Ошибка',ex)
        exit('Чета не так')
    
    await producer.start()
    try:
        await producer.send_and_wait(
            "user_requests", value=json_util.dumps(data).encode("utf-8")
        )
        print('Отправили реквест')
    finally:
        await producer.stop()
