from fastapi import APIRouter
from schemas.book import Book
from aiokafka import AIOKafkaProducer
import json
import os
from pydantic import BaseModel

router = APIRouter(tags=["books"])

KAFKA_URL = os.getenv("KAFKA_URL")


@router.post("/book")
async def acquire_book_info(book: Book):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_URL)
    await producer.start()

    try:
        await producer.send_and_wait(
            "book_data", json.dumps(book.__dict__).encode("utf-8")
        )
        return {"message": "data saved"}
    finally:
        await producer.stop()


@router.get("/book")
async def get_books(): ...

class Message(BaseModel):
    action:str
    body:dict
    
class Data(BaseModel):
    request_id:str
    message:Message
    
    
@router.post('/data')
async def send_data(data:Data):
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_URL,
    )
    await producer.start()
    
    try: 
        await producer.send_and_wait('notification_requests',data.model_dump_json().encode('utf-8'))
        # return {'message':}
        return {'message':'data sent'}
    finally:
        await producer.stop()