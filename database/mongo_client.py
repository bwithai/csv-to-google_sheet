import pymongo

import utils

# Establish a connection to the MongoDB server
client = pymongo.MongoClient(utils.get_database_url())

# Create/select a database
db = client["big-data"]


def get_collection(name):
    # Create/select a collection
    collection = db[name]
    return collection


def close_mongo_client():
    print("db connection is closed due to progress complete")
    client.close()
