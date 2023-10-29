import os
import json
import sys
import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud.exceptions import Conflict
import time
import logging 
from logging.handlers import TimedRotatingFileHandler

from dotenv import load_dotenv
if len(sys.argv)==1:
    raise Exception('Log directory is missing .')

log_directory=sys.argv[1]

def setup_logger():
    log_filename = "extract_ingest_bigquery_"
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    file_handler = TimedRotatingFileHandler(os.path.join(log_directory, log_filename), when='midnight',
                                             interval=1, backupCount=0, encoding='utf-8', delay=False)
    file_handler.suffix = "%Y%m%d.log"
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger

logger=setup_logger()
# load env variables

load_dotenv()
grant_type= os.getenv("grant_type")
client_id=os.getenv("client_id")
client_secret=os.getenv("client_secret")
base_url = os.getenv("base_url")
token_url = os.getenv("token_url")
key_path=os.getenv('key_path')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path
wait_timeout = 300  # 5 min
project_id = "irn-71774-ope-57"
dataset_id = "db_raw_irn_71774_cvb"
table_id = "cvb_css_virtuel_key"
filters = [
    "vkEcuMgt",
    "popTokenReques",
    "SpCmdSend",
    "SpCmdRsp",
    "SpCmdFail",
    "SpNewRightError",
    "SpNewRightGranted"
    ]
schema = [
    bigquery.SchemaField("creationDate", "TIMESTAMP"),
    bigquery.SchemaField("collectionDate", "TIMESTAMP"),
    bigquery.SchemaField("producer", "STRING"),
    bigquery.SchemaField("vkecuId", "STRING"),
    bigquery.SchemaField("log_type", "STRING"),
    bigquery.SchemaField("parameters", "STRING"),
]

# -----------------------------Functions----------------------------

def get_api():
    token_response = requests.post(
    token_url,
     data={
    "grant_type": grant_type,
    "client_id": client_id,
    "client_secret": client_secret
    })
    token_data = token_response.json()
    access_token = token_data["access_token"]
    data = token_response.json()
    headers = {
        'Authorization': f'Bearer {access_token}'
    }
    return base_url,headers
def get_data(filters):
    data_to_insert=[]
    logger.info('Getting API CREED ...')
    base_url,headers=get_api()

    for filter_type in filters:
        url = f'{base_url}?filter%5Btype%5D={filter_type}'
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status() 
            data = response.json()
            log_records = data.get("data", [])
            for record in log_records:
                attributes = record.get("attributes", {})
                try:
                    creation_date = attributes["creationDate"]
                    collection_date = attributes["collectionDate"]
                    producer = attributes["producer"]
                    vkecu_id = attributes["vkecuId"]
                    log_type = attributes["type"]
                except KeyError as e:
                    logger.error(f"KeyError: {e}")
                parameters = attributes.get("parameters", {})
                parameters=str(parameters)
                row = (creation_date, collection_date, producer, vkecu_id, log_type,parameters)
                data_to_insert.append(row)
        except requests.exceptions.RequestException as e:
            logger.error("Request Exception {} .".format(e))
        except ValueError as e:
            logger.error("JSON Parsing Exception {} .".format(e))

        except Exception as e:
            logger.error("An unexpected exception occurred {} .".format(e))
    logger.info('Total of records retreived : {}'.format(len(data_to_insert))) 
    return data_to_insert
def delete_create_table(client,table_ref,schema,wait_timeout=300):
    logger.info("Attempt to delete the table if it exists.")
    try:
        client.delete_table(table_ref)
        logger.info(f"Table '{table_id}' deleted.")
    except NotFound:
        logger.error(f"Table '{table_id}' does not exist, so it wasn't deleted.")

    try:
        logger.info("Creating the new table.")
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        logger.info(f"Table '{table_id}' created.")
    except Conflict as e:
        logger.error("Table {} can't not be created du to the conflict - {} ".format(table_id,e))
    return table

def insertion_available(table_ref,client):
    logger.info("Start time for retreiving table and insertion.")
    # for now we will work with while loop until we find the avg time of insertion and availabilty of table 
    start_time = time.time()
    while True:
        try:
            client.get_table(table_ref)
            logger.info("Table is available.")
            return True  # The table is available, exit the loop
        except NotFound:
            if time.time() - start_time >= wait_timeout:
                logger.error("Table did not become available within the timeout of {}.".format(wait_timeout))
                return False
            logger.warning("Table not available yet. Waiting for {} seconds...".format(5))
            time.sleep(5)

def insert_data(table,client,data,wait_timeout=300):
    if not table.schema:
        logger.error("Table schema is empty. Aborting data insertion.")
        return False
    else:
        start_time = time.time()
        i=0
        while True:
            try:
                errors = client.insert_rows(table, data)
                logger.info("Data has been inserted successfully.")
                break
            except NotFound:
                if time.time() - start_time >= wait_timeout:
                    logger.error("The insertion did not become available within the timeout {} seconds.".format(wait_timeout))
                    return False
            logger.warning("Insertion not available yet. Waiting  for {} seconds...".format(5))
            i+=1
            time.sleep(5) 
        logger.info( "The vailability of insertion takes  {} seconds.".format(str(i*5)))
        if not errors:
            logger.info("Data inserted successfully.")
            end = time.time()
            logger.info("Total of Time consummed to insert Data successfully is {} seconds .".format(str(end-start_time)))
            return True
        else:
            logger.error("Error inserting data: {}".format(errors))
            end = time.time()
            logger.info("Total of Time consummed to insert Data unsuccessfully is {} seconds .".format(str(end-start_time)))
            return False

if __name__=="__main__":
    logger.info("\n---------------Start---------------\n")
    client = bigquery.Client(project=project_id)
    table_ref = client.dataset(dataset_id).table(table_id)

    table=delete_create_table(client,table_ref,schema,wait_timeout=300)
    is_available=insertion_available(table_ref,client)
    if is_available:
        data=get_data(filters)
        inserted=insert_data(table,client,data)
    else:
        print("Availability of table is ",is_available)
