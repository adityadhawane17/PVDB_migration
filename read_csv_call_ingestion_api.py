import csv
import requests
import json
from config import *
from utils import Utils
import psycopg2
import os
import uuid
import logging as lg
import base64

def read_csv_file(filename, folder_path):
    json_objects = []
    with open(filename, "r", encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader) #skip the column header value
        rows = []
        print("Reading CSV file--------")
        for row in reader:
            if row:
                #print(row)
                json_object = {}
                json_object["token"] = row[0]
                json_object["payment_vehicle_id"] = row[1]
                json_object["tenant_id"] = row[2]
                json_object["value"] = row[3]
                encrypted_val = json_object["value"]
                size = len(encrypted_val)
                json_object["value"] = encrypted_val[2:size-1]
                #print("Len of encrypted_val: ")
                #print(len(encrypted_val[2:size-1]))
                #print(encrypted_val)
                #print(type(encrypted_val))
                json_objects.append(json_object)
                #rows.append(row[3]) #column index
    return json_objects

def log_payload_to_csv(filename, payload, error_message):
    keys = payload.keys()
    values = payload.values()
    writer.writerow(list(keys) + ['Error Message'])

    with open('api_error_log.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(list(values)+[error_message])

def make_post_api_call(filename,logger,url, body, key):
    logger.info("Making an API call to Ingestion API")
    request_body = {}
    request_body["token"] = body["token"]
    request_body["isSearchable"] = "true"
    #Convert Hex encrypted string back to bytes
    #encrypted_string = base64.b16encode(bytes.fromhex(body["value"]))
    #CALL DECRYPT FUNCTION FOR VALUE
    decrypted_value = Utils.cryptography_handler(body["value"], False)
    request_body["value"] = ""#decrypted_value

    #api-key-value = ""
    correlation_id = str(uuid.uuid4())
    Headers = {"x-api-key" : "2ad66b9d-63fb-4ea2-819b-3f7e89c59f7b", "Content-Type" : "application/json", "x-correlation-id" : correlation_id, "x-tenant-id" : body["tenant_id"]}
    print(url)
    print(Headers)
    print(request_body)
    response = requests.post(url, json=request_body, headers=Headers)
    if not str(response.status_code).startswith('2'):
        payload_body = response.json()
        error_message = response.text
        log_payload_to_csv(filename,payload_body, error_message)
    print("Ingestion Response: ")
    print(response.text)
    return response

def operate_on_ingestion_csv(filename, logger,req_body, properties):
    #MAKE POST API CALL TO INGESTION CAPILLARY-API
    logger.info("Operating on Ingestion input CSV file")
    url = properties["ingestion_api_url"]
    cipherKey = properties["key"]
    for body in req_body:
        response = make_post_api_call(filename, logger,url, body, cipherKey)
    #print("RESPONSE FROM API: ")
    #api_response = response.json()
    #print(api_response)

def create_json_objects(rows):
    json_objects = []
    print("len of rows:")
    print(len(rows))
    for row in rows:
        json_object = {}
        json_object["Token"] = row
        json_objects.append(json_object)
    return json_objects

def main():
    properties = getconfig(filename="properties.ini",section='api-db-migration')
    lg.basicConfig(
        filename=properties["payment_vehicle_log_file_location"],
        level=lg.DEBUG,
        format="%(asctime)s:%(levelname)s:%(message)s"
    )
    logger=lg.getLogger("loadPV_data")
    logger.info("read_csv_call_ingestion_API_Starting.........................................................................")
    logger.info("Read_csv_call_ingestion_API job is starting.")
    folder_path = properties["ingestion_input_filepath"]
    csv_files = os.listdir(folder_path)
    dv_api_filename = properties["dv_api_logs_filepath"]
    logger.info("Number of csv_files found at this location "+folder_path+" ="+str(len(csv_files)))
    rows = []
    for csv_file in csv_files:
        req_body = read_csv_file(folder_path+"\\"+csv_file, folder_path)
        #req_body = create_json_objects(rows)
        operate_on_ingestion_csv(dv_api_filename,logger,req_body, properties)
    logger.info("Read_csv_call_ingestion_API job is Completed.")
if __name__ == "__main__":
    main()