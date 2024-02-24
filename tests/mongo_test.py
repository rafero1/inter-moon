from pymongo.mongo_client import MongoClient

db = MongoClient("mongodb://0.0.0.0:27017/")

print(db.list_database_names())
