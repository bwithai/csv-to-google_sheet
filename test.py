from database.mongo_client import db
from database.queries import find_column_name_not_empty, create_indexing

collection = db["Australia"]

create_indexing(collection)


def get_mobile():
    find_column_name_not_empty("mobile", collection)


def get_mobile_number():
    find_column_name_not_empty("phone_numbers", collection)
