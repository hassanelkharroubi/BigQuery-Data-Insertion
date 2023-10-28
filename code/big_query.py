import os
import json
import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud.exceptions import Conflict
import time
import logging
from dotenv import load_dotenv

logging.basicConfig(filename='../logs/bigquery_execution.log', 
                    encoding='utf-8',
                      level=logging.DEBUG,
                      format='%(asctime)s - %(levelname)s - %(message)s',
                      datefmt='%Y-%m-%d %H:%M:%S'
                      )
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
wait_timeout = 300  # 5 min
project_id = "irn-71774-ope-57"
dataset_id = "db_raw_irn_71774_cvb"
table_id = "cvb_css_virtuel_key"
filters = [
    "vkEcuMgt",
    #"popTokenReques",
    #"SpCmdSend",
    #"SpCmdRsp",
    #"SpCmdFail",
    #"SpNewRightError",
    #"SpNewRightGranted"
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
    print('getting API CREED ...')
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
                    print(f"KeyError: {e}")
                parameters = attributes.get("parameters", {})
                parameters=str(parameters)
                row = (creation_date, collection_date, producer, vkecu_id, log_type,parameters)
                data_to_insert.append(row)
        except requests.exceptions.RequestException as e:
            print("Request Exception:", e)
            return []
        except ValueError as e:
            print("JSON Parsing Exception:", e)
            return []
        except Exception as e:
            print("An unexpected exception occurred:", e)
            return []
    return data_to_insert
def delete_create_table(project_id,dataset_id,table_id,schema,wait_timeout=300):
    start = time.time()
    logging.info("Instantiate BigQuery client instance")
    client = bigquery.Client(project=project_id)
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
    return table_ref,client,table

def insertion_available(table_ref,client):
    logging.info("Start time for retreiving table and insertion.")
    # for now we will work with while loop until we find the avg time of insertion and availabilty of table 
    start_time = time.time()
    while True:
        try:
            client.get_table(table_ref)
            logging.info("Table is available.")
            return True  # The table is available, exit the loop
        except NotFound:
            if time.time() - start_time >= wait_timeout:
                logging.error("Table did not become available within the timeout of {}.".format(wait_timeout))
                return False
            logging.warning("Table not available yet. Waiting for {} seconds...".format(5))
            time.sleep(5)

def insert_data(table,client,data,wait_timeout=300):
    if not table.schema:
        logging.error("Table schema is empty. Aborting data insertion.")
        return False
    else:
        start_time = time.time()
        i=0
        while True:
            try:
                errors = client.insert_rows(table, data)
                logging.info("Data has been inserted successfully.")
                break
            except NotFound:
                if time.time() - start_time >= wait_timeout:
                    logging.error("The insertion did not become available within the timeout {} seconds.".format(wait_timeout))
                    return False
            logging.warning("Insertion not available yet. Waiting  for {} seconds...".format(5))
            i+=1
            time.sleep(5) 
        logging.info( "The vailability of insertion takes  {} seconds.".format(str(i*5)))
        if not errors:
            logging.info("Data inserted successfully.")
            end = time.time()
            logging.info("Total of Time consummed to insert Data successfully is {} seconds .".format(str(end-start_time)))
            return True
        else:
            logging.error("Error inserting data: {}".format(errors))
            end = time.time()
            logging.info("Total of Time consummed to insert Data unsuccessfully is {} seconds .".format(str(end-start_time)))
            return False

if __name__=="__main__":
    table_ref,client,table=delete_create_table(project_id,dataset_id,table_id,schema,wait_timeout=300)
    is_available=insertion_available(table_ref,client)
    if is_available:
        data=get_data(filters)
        inserted=insert_data(table,client,data)
    else:
        print("Availability of table is ",is_available)
