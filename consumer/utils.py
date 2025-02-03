from db import app
import pymongo
from datetime import datetime
from bson import ObjectId, json_util
import json 

async def db_get_users(body=None):
    """ Получение всех юзеров из монго """
    client = pymongo.MongoClient("mongodb://root:example@mongo:27017/")
    raw_users = client.app.users.find()
    users = list(raw_users)
    # for raw_user in raw_users:
    #     raw_user['_id'] = str(raw_user['_id'])
    #     users.append(raw_user)
    
    return users


async def db_add_notification(message):
    """ Добавление нового уведомления в монго"""
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
    client = pymongo.MongoClient("mongodb://root:example@mongo:27017/")
    users = client.app.notifications.insert_one(data).inserted_id
    return users


async def db_read_notification(message):
    """ Запись новой информации в уведомления"""
    body = message['message']['body']
    update_data = {'$set':{'is_read':True, 'read_at':datetime.now()}}
    notification = app.notifications.update_one(filter={'_id': ObjectId(body['notification_id'])},update=update_data)
    
    return str(notification)
    