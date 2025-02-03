from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
import asyncio
import json
import os
from utils import db_get_users, db_add_notification
from bson import json_util


KAFKA_URL = os.getenv("KAFKA_URL")


async def get_users(message):
    await db_get_users(message['message']['body'])


async def add_notifications(body): 
    id = await db_add_notification(body)
    data = {
        'request_id':body['request_id'],
        'message':{
            'action':'add_notification',
            'body':{
                'message':'success',
                'notification_id':id
            }
        }
    }
    
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_URL
    )
    await producer.start()
    
    await producer.send_and_wait('notification_responses', value=json.dumps(data).encode('utf-8'))
    

async def process_data(action, body=None):
    """ """
    actions = {
        "add_notification": add_notifications,
        "get_users": get_users,
    }
    print(action)
    if action in actions:
        return await actions[action](body) if actions[action] else None 
    else:
        ...#TODO Сделать выброс в DLQ


async def consume_data():
    consumer = AIOKafkaConsumer(
        'notification_requests',
        group_id="data_consumber",
        bootstrap_servers=KAFKA_URL,
    )
    await consumer.start()
    

    try:
        async for msg in consumer:
            raw_data = msg.value.decode('utf-8')
            print('rawdata', raw_data)
            msg_data = json.loads(json_util.dumps(raw_data))
            print(msg_data)
            if msg_data['message']['action'] is None:
                print(msg_data)
            await process_data(
                action=msg_data["message"]["action"], body=msg_data
            )
            
    finally:
        await consumer.stop()


async def main():
    try:
        asyncio.create_task(await consume_data())
    except KafkaConnectionError as ex:
        print(ex)


if __name__ == "__main__":
    asyncio.run(main())
