from pymongo import MongoClient


def mongo_connection():
    connection_string = "mongodb://localhost:27017"
    client = MongoClient(connection_string)
    return client

def tweetsConnection():
    return mongo_connection()['sseramemesTweets']['tweets']









