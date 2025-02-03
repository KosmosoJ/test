import pymongo

client = pymongo.MongoClient("mongodb://root:example@mongo:27017/") # Инициализация клиента для монго 

app = client.app # Подключение к бд приложения