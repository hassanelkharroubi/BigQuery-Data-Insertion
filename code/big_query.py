import os
import json
import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud.exceptions import Conflict
import time
import logging

# wait time for availabilty of table insertion 
wait_timeout = 300  # 5 min

logging.basicConfig(filename='bigquery_execution.log', 
                    encoding='utf-8',
                      level=logging.DEBUG,
                      format='%(asctime)s - %(levelname)s - %(message)s',
                      datefmt='%Y-%m-%d %H:%M:%S'
                      )
from dotenv import load_dotenv

# load env variables
logging.info("Import ENV variables")
load_dotenv()
grant_type= os.getenv("grant_type")
client_id=os.getenv("client_id")
client_secret=os.getenv("client_secret")
base_url = os.getenv("base_url")
token_url = os.getenv("token_url")
key_path=os.getenv('key_path')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path

start = time.time()
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
    )
]

logging.info("Instantiate BigQuery client instance")
client = bigquery.Client(project=project_id)

schema = [
    bigquery.SchemaField("creationDate", "TIMESTAMP"),
    bigquery.SchemaField("collectionDate", "TIMESTAMP"),
    bigquery.SchemaField("producer", "STRING"),
    bigquery.SchemaField("vkecuId", "STRING"),
    bigquery.SchemaField("log_type", "STRING"),
    bigquery.SchemaField("parameters", "STRING"),
]


logging.info("Attempt to delete the table if it exists.")
table_ref = client.dataset(dataset_id).table(table_id)
try:
    client.delete_table(table_ref)
    logging.info(f"Table '{table_id}' deleted.")
except NotFound:
    logging.error(f"Table '{table_id}' does not exist, so it wasn't deleted.")

try:
    logging.info("Creating the new table.")
    table = bigquery.Table(table_ref, schema=schema)
    table = client.create_table(table)
    logging.info(f"Table '{table_id}' created.")
except Conflict as e:
    logging.error("Table {} can't not be created du to the conflict - {} ".format(table_id,e))


start_time = time.time()
logging.info("Start time for retreiving table and insertion.")

# for now we will work with while loop until we find the avg time of insertion and availabilty of table 

while True:
    try:
        client.get_table(table_ref)
        logging.info("Table is available.")
        break  # The table is available, exit the loop
    except NotFound:
        if time.time() - start_time >= wait_timeout:
            logging.error("Table did not become available within the timeout of {}.".format(wait_timeout))
            break
        logging.warning("Table not available yet. Waiting for {} seconds...".format(5))
        time.sleep(5)

if not table.schema:
    logging.error("Table schema is empty. Aborting data insertion.")
    logging.info("The operation of searhing for table {} takes {} seconds. ".format(table_id,time.time()-start_time))
else:
    start_time = time.time()
    i=0
    while True:
        try:
            errors = client.insert_rows(table, data)
            break  # The insertion is available, exit the loop
        except NotFound:
            if time.time() - start_time >= wait_timeout:
                logging.error("The insertion did not become available within the timeout {} seconds.".format(wait_timeout))
                break
        logging.warning("Insertion not available yet. Waiting  for {} seconds...".format(5))
        i+=1
        time.sleep(5)  # Wait for 5 seconds before checking again
    logging.info( "Insertion of time to be available is  ",str(i*5)," seconds")
    if not errors:
        logging.info("Data inserted successfully.")
        end = time.time()
        logging.info("Total of Time consummed to insert Data successfully is {} seconds .".format(str(end-start)))
    else:
        logging.error("Error inserting data: {}".format(errors))
        end = time.time()
        logging.info("Total of Time consummed to insert Data unsuccessfully is {} seconds .".format(str(end-start)))
