from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from config.config import KAFKA_CONSUMER_CONF, KAFKA_PASSWORD,KAFKA_PRODUCER_CONF,KAFKA_URL,KAFKA_USER
from db import app 

async def process_users_data(request_id):
    user_request = await produce_user_request(id) #TODO 
    user_data = await consume_users_data() #TODO
    return #TODO return user
    ...
    
async def consume_users_data(user_id):
    consumer = AIOKafkaConsumer('user_responses',KAFKA_URL, **KAFKA_CONSUMER_CONF)
    
    await consumer.start()
    try:
        ...
    finally:
        await consumer.stop()
    
    
async def produce_user_request(user_id):
    producer = AIOKafkaProducer('user_requests', KAFKA_URL, **KAFKA_PRODUCER_CONF)
    
    await producer.start()
    
    data = {
        {
	"request_id": "2449346b-98fe-49e6-9088-a5d1d0fcfc76",
	"message": {
		"action": "read_user",
		"body": {
            'user_id':user_id
		}
	}
}
    }
    
    try:
        producer
    finally:
        producer.stop()
    


    