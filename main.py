import re
import csv
import os
import time
from tqdm import tqdm
from database.models import add_person
from database.mongo_client import get_collection
from utils import get_person_data_from_csv, GREEN, END, business_email_found, industry_pattern

data_directory = 'ZoomInfo'
dist_path = os.path.join(os.getcwd(), data_directory)

if not os.path.exists(dist_path):
    print(f"Directory name {data_directory} not found")

batch_size = 50  # Define the batch size

name = "Australia"
skip_uploaded_csv = True

for filename in os.listdir(data_directory):
    if filename.endswith('.csv'):
        print(filename, " is in progress.....")
        collection_name = filename.split(".")
        if skip_uploaded_csv:
            if collection_name[0] == "Belgium":
                skip_uploaded_csv = False
            else:
                print(collection_name[0], "is skiped")
                continue
        print("collection name: ", collection_name[0])

        # Create/select a collection
        collection = get_collection(collection_name[0])

        # first iteration skip flag
        skip = True

        start = time.time()
        with open(f"ZoomInfo/{filename}", newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            total_rows = sum(1 for row in reader)  # Count the total number of rows
            csvfile.seek(0)  # Reset the file position for reading

            batch = []  # Initialize an empty batch list

            # Use tqdm to create a colorful progress bar with percentage
            for row in tqdm(reader, desc=f"{GREEN}Inserting rows{END}", total=total_rows, ncols=100):
                # Skip 1st iteration b/c its column name
                if skip:
                    skip = False
                    continue

                # Each person data from csv
                person = get_person_data_from_csv(row)

                if not business_email_found(person):
                    continue

                # Add filtering logic here using regex patterns for industries and emails
                if not re.match(industry_pattern, person.industry):
                    continue

                batch.append(person.as_dict())  # Add the person to the batch

                if len(batch) >= batch_size:
                    # Insert the batch into the collection
                    result = collection.insert_many(batch)
                    batch = []  # Clear the batch after insertion

            # Insert any remaining rows in the batch
            if batch:
                result = collection.insert_many(batch)
        end = time.time()
        print(filename, f" Took {round(end - start, 2)} s. to stored in DB :)\n\n")
