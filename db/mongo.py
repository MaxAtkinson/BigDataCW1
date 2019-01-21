from pymongo import MongoClient

client = MongoClient()
client.drop_database('chinook')
db = client.chinook
