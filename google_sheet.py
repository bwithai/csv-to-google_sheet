import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

from tqdm import tqdm

from database.mongo_client import db
from database.queries import collections, create_indexing, get_all_documents_from_db
from utils import GREEN, END

# Define the scope and load credentials from the JSON key file
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name("gc_credential/softtik-technologies-5a8fde70b661.json", scope)
client = gspread.authorize(credentials)

# Open the Google Sheet by its title or URL
sheet = client.open("BigData").sheet1

for collection_name in collections:
    if collection_name != "Albania":
        continue
    print("\t\t  ", collection_name, " is in progress...")
    collection = db[collection_name]

    create_indexing(collection)

    data = get_all_documents_from_db(collection_name)
    rows_to_insert = [list(item.values()) for item in data]  # Prepare the batch of rows

    # Determine the total number of rows to insert
    total_rows = len(rows_to_insert)

    # Check if the batch size is greater than the number of documents
    batch_size = min(1000, len(rows_to_insert))

    # Create a tqdm progress bar
    progress_bar = tqdm(total=total_rows, desc=f"{GREEN}Inserting {collection_name} rows into GoogleSheet{END}", ncols=100)

    # Measure execution time
    start_time = time.time()

    # Insert data into the Google Sheet in batches
    for i in range(0, total_rows, batch_size):
        batch = rows_to_insert[i:i + batch_size]
        sheet.insert_rows(batch, 2)  # Specify the row number where data should be inserted

        # Adjust batch size if needed for the remaining rows
        remaining_rows = total_rows - (i + batch_size)
        if remaining_rows < batch_size:
            batch_size = remaining_rows

        # Update the progress bar
        progress_bar.update(len(batch))

        # Pause for a while (e.g., 30 seconds) between batches to avoid rate limits
        time.sleep(15)

    # Close the progress bar
    progress_bar.close()

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"All the documents of the {collection_name} stored in sheet in {execution_time} seconds\n\n")