from fastapi import APIRouter
from schemas.book import Book
from aiokafka import AIOKafkaProducer
import json
import os

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
