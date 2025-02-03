from db import app
import pymongo
from datetime import datetime
from bson import json_util


async def db_get_users(body=None):
    client = pymongo.MongoClient("mongodb://root:example@mongo:27017/")
    users = client.app.users.find()
    for user in users:
        print(user)


async def db_add_notification(message):
    body = message["message"]["body"]
    data = {
        "user_id": body["user_id"],
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
    client = pymongo.MongoClient("mongodb://root:example@mongo:27017/")
    users = client.app.notifications.insert_one(data).inserted_id
    return users
