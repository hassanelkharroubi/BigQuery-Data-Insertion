import os
import json
import requests
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import time
from dotenv import load_dotenv

# load env variables
load_dotenv()
grant_type= os.getenv("grant_type")
client_id=os.getenv("client_id")
client_secret=os.getenv("client_secret")
base_url = os.getenv("base_url")
token_url = os.getenv("token_url")
key_path=os.getenv('key_path')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path

start = time.time()

client = bigquery.Client()

project_id = "irn-71774-ope-57"
dataset_id = "db_raw_irn_71774_cvb"
table_id = "cvb_css_virtuel_key"
    
data = [
    (
        "2023-10-26T12:00:00Z",
        "2023-10-26T14:30:00Z",
        "Producer1",
        "vkecuId1",
        "Info",
        '{"param1": "value1", "param2": "value2"}'
    ),
    (
        "2023-10-27T08:15:00Z",
        "2023-10-27T10:45:00Z",
        "Producer2",
        "vkecuId2",
        "Error",
        '{"error_code": 500, "message": "An error occurred"}'
    ),
    # Add more sample data rows as needed
]
client = bigquery.Client(project=project_id)

schema = [
    bigquery.SchemaField("creationDate", "TIMESTAMP"),
    bigquery.SchemaField("collectionDate", "TIMESTAMP"),
    bigquery.SchemaField("producer", "STRING"),
    bigquery.SchemaField("vkecuId", "STRING"),
    bigquery.SchemaField("log_type", "STRING"),
    bigquery.SchemaField("parameters", "STRING"),
]


# Attempt to delete the table if it exists
table_ref = client.dataset(dataset_id).table(table_id)
try:
    client.delete_table(table_ref)
    print(f"Table '{table_id}' deleted.")
except NotFound:
    print(f"Table '{table_id}' does not exist, so it wasn't deleted.")

# Create the new table
table = bigquery.Table(table_ref, schema=schema)
table = client.create_table(table)
print(f"Table '{table_id}' created.")

wait_timeout = 300  
start_time = time.time()

while True:
    try:
        client.get_table(table_ref)
        print("Table is available.")
        break  # The table is available, exit the loop
    except NotFound:
        if time.time() - start_time >= wait_timeout:
            print("Table did not become available within the timeout.")
            break
        print("Table not available yet. Waiting...")
        time.sleep(5)  # Wait for 5 seconds before checking again

if not table.schema:
    print("Table schema is empty. Aborting data insertion.")
else:
    wait_timeout = 300  
    start_time = time.time()
    i=0
    while True:
        try:
            errors = client.insert_rows(table, data)
            break  # The insertion is available, exit the loop
        except NotFound:
            if time.time() - start_time >= wait_timeout:
                print("The insertion did not become available within the timeout.")
                break
        print("Insertion not available yet. Waiting...")
        i+=1
        time.sleep(5)  # Wait for 5 seconds before checking again
    print( "Insertion of time to be available is  ",str(i*5)," seconds")
    if not errors:
        print("Data inserted successfully.")
        end = time.time()
        print("Total of Time consummed to insert Data successfully is ",str(end-start)," seconds .")
    else:
        print("Error inserting data: {}".format(errors))
        end = time.time()
        print("Total of Time consummed to insert Data unsuccessfully is ",str(end-start)," seconds .")
