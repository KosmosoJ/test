import urllib.parse
from pymongo import MongoClient
from dotenv import load_dotenv, find_dotenv
import os

if find_dotenv():
    load_dotenv()
    host = os.getenv("MONGO_HOST")
    user = os.getenv("MONGO_USER")
    password = os.getenv("MONGO_PASS")
    db = os.getenv("MONGO_DB")
    
else:
    exit(".env не создан")

try:
    client = MongoClient(
        f"mongodb://{user}:{urllib.parse.quote_plus(password)}@{host}/{db}",
        serverSelectionTimeoutMS=5000,
    )
    client.server_info()  # Проверяем соединение
    print("MongoDB подключена")
except Exception as e:
    print(urllib.parse.quote_plus(password))
    exit(f"Ошибка подключения к MongoDB: {e}")


app = client.notifications  # Подключение к бд приложения

