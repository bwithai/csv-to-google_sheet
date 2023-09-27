from database.schemas import PersonSchema


def add_person(person: PersonSchema, collection):
    # Insert the entire list with timestamp
    insert_result = collection.insert_one(person.as_dict())
    return insert_result.inserted_id
