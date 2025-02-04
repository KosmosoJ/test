import urllib.parse
from pymongo import MongoClient
from config.config import MONGO_DB, MONGO_HOST, MONGO_PASSWORD, MONGO_USER

try:
    client = MongoClient(
        f"mongodb://{MONGO_USER}:{urllib.parse.quote_plus(MONGO_PASSWORD)}@{MONGO_HOST}/{MONGO_DB}",
        serverSelectionTimeoutMS=5000,
    )
    client.server_info()  # Проверяем соединение
    print("MongoDB подключена")
except Exception as e:
    exit(f"Ошибка подключения к MongoDB: {e}")

app = client.notifications  # Подключение к бд приложения

