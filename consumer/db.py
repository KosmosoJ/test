import pymongo

client = pymongo.MongoClient('mongodb://root:example@localhost:27017/')

app = client.app