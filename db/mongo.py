from pymongo import MongoClient

''' Connects to mongo, drops chinook db so it can be run multiple times. Exports db. '''
client = MongoClient()
client.drop_database('chinook')
db = client.chinook
