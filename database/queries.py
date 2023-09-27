import time

import pymongo

from database.mongo_client import db

collections = ["Albania", "Algeria", "Andorra", "Argentina", "Armenia", "Australia", "Austria", "Azerbaijan",
               "Bahamas", "Bahrain", "Bangladesh", "Belarus", "Belgium", "Belize", "Benin", "Bhutan", "Bolivia",
               "Botswana",
               ]


# collection = db["Albania"]


def create_indexing(collection):
    index_fields = ["job_title", "industry"]

    # Create indexes for the collection
    print("Indexing is in progress...")
    start_idx = time.time()
    for field in index_fields:
        collection.create_index([(field, pymongo.ASCENDING)])
    end_idx = time.time()
    print(f"Indexing Took {round(end_idx - start_idx, 2)} s.")


def find_column_name_not_empty(name, collection):
    query = {
        name: {"$ne": ""},
    }

    # Define the projection
    projection = {
        "_id": 0,  # Exclude the _id field
        "full_name": 1,
        "emails": 1,
        name: 1,
        # Add other fields you want to retrieve
    }

    results = collection.find(query, projection).limit(5)

    print("Result:")
    for result in results:
        print(result)


def get_all_documents_from_db(collection_name):
    # Define the projection
    projection = {
        "_id": 0,  # Exclude the _id field
        "full_name": 1,
        "industry": 1,
        "job_title": 1,
        "emails": 1,
        "country": collection_name
        # Add other fields you want to retrieve
    }

    # Query the collection and retrieve all documents with the specified projection
    print("Query is in progress...")
    start = time.time()
    documents = db[collection_name].find({}, projection)
    end = time.time()
    print(f"query Took {round(end - start, 2)} s.")

    return documents


def delete_invalid_emails(collection):
    query = {
        "emails": {
            "$not": {
                "$regex": "@"
            }
        }
    }
    result = collection.delete_many(query)
    # Print the number of documents deleted
    print(f"Deleted {result.deleted_count} documents. \n\n")


# Query documents where email is not null and job_title contains 'developer'
# for return record having value is empty: todo "$eq": ""
# query = {
#     "mobile": {"$ne": ""},
# "phone_numbers": {"$ne": ""},
# "emails": {"$ne": ""},
# "emails": {
#     "$not": {
#         "$regex": r"@(hotmail\.com|gmail\.com|yahoo\.com)$",
#         "$options": "i"  # Case-insensitive
#     },
#     "$ne": ""
# },
# # "job_title": {"$regex": "professor", "$options": "i"},  # Case-insensitive regex
# "industry": {"$regex": "software", "$options": "i"}
# }
