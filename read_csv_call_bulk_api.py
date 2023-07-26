import csv
import requests
import json
from config import *
from utils import Utils
import psycopg2
import os
import logging as lg
import base64

def set_logging(name):
    lg.getLogger(name)

def read_csv_file(filename, folder_path):
    file = folder_path+"\\"+filename
    with open(file, "r") as f:
        reader = csv.reader(f)
        next(reader) #skip the column header value
        rows = []
        for row in reader:
            rows.append(row[2]) #column index
    return rows

def make_post_api_call(url, body):
    Headers = {"x-api-key" : "7LAPuJqGQApn1lxES6xTqC1sjkxVtfcX", "Content-Type" : "application/json"}
    response = requests.post(url, json=body, headers=Headers)
    return response

def store_api_response_in_csv_file(filename, response):
	with open(filename, "w") as f:
		writer = csv.writer(f)
		for row in response:
			writer.writerow(row)

def create_json_objects(rows):
    json_objects = []
    for row in rows:
        json_object = {}
        json_object["Token"] = row
        json_objects.append(json_object)
    return json_objects

def operate_on_paymentVehicle_csv(logger,req_body, properties, list_of_Inputs):
    #MAKE POST API CALL TO DATAVAULT BULK-API
    logger.info("Operating on Payment vehicle CSV file")
    url = properties["datavault_bulk_get_token_api_url"]
    response = make_post_api_call(url, req_body)
    api_response = response.json()
    for record in api_response:
        accNo = record['Value']
        print("CardNumbers: ")
        print(accNo)
        dvID = record['Token']
        conn = None
        query = "SELECT data_vault_id, payment_vehicle_id, tenant_id FROM {} WHERE data_vault_id = '{}'".format("payment_vehicles.payment_vehicle", dvID)

        #CONNECT TO PAYMENT_VEHICLE DB, FETCH DATA BASED ON DataVault_ID & POPULATE CSV WITH THAT DATA
        try:
            params=dbconfig(filename="database.ini",section="payment_vehicle")
            conn = psycopg2.connect(**params)
            db_cursor = conn.cursor()
            db_cursor.execute(query)
            row = db_cursor.fetchone()
            db_cursor.close()
            pvID = row[1]
            tenantID = row[2]
            input_data = {
                "data_vault_id": dvID,
                "payment_vehicle_id": pvID,
                "tenant_id": tenantID,
                "value": accNo
            }
            input_data_JSON = json.dumps(input_data)
            list_of_Inputs.append(input_data_JSON)
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error("Error fetching count for {tablename}".format(tablename=params["database"]))
            logger.error(error)
        except psycopg2.Error as e:
            logger.error("Error fetching count for {tablename}".format(tablename=params["database"]))
            t_message = "Error: " + e
            logger.error(t_message)
        finally:
            if conn is not None:
                logger.info("Closing the DB connection.")
                conn.close()


def create_csv_for_ingestion(logger,list_of_Inputs, filename,key):
    logger.info("Creating CSV file for Ingestion")
    with open(filename, "w", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["data_vault_id", "payment_vehicle_id", "tenant_id", "value"])
        for json_object in list_of_Inputs:
            obj = json.loads(json_object)
            #MAKE CALL TO ENCRYPT FUNCTION FOR VALUE
            logger.info("Calling the cryptography_handler function")
            print("Val to be encrypted: ")
            print(obj['value'])
            print(len(obj['value']))
            encrypted_value = Utils.cryptography_handler(obj['value'],True)
            print("Encrypted val: ")
            print(encrypted_value)
            print(len(encrypted_value))
            #Convert encrypted_value into Hex
            #encrypted_value_hex = base64.b16encode(encrypted_value).decode()
            writer.writerow([obj['data_vault_id'], obj['payment_vehicle_id'], obj['tenant_id'], encrypted_value])


def main():
    properties = getconfig(filename="properties.ini",section='api-db-migration')
    lg.basicConfig(
        filename=properties["payment_vehicle_log_file_location"],
        level=lg.DEBUG,
        format="%(asctime)s:%(levelname)s:%(message)s"
    )
    logger=lg.getLogger("loadPV_data")
    set_logging("read_csv_call_bulk_API_Starting.........................................................................")
    logger.info("Read_csv_call_bulk_API job is starting.")

    folder_path = properties["pv_db_datafolder"]
    cipherKey = properties["key"]
    csv_files = os.listdir(folder_path)
    logger.info("Number of csv_files found at this location- "+folder_path+" ="+str(len(csv_files)))
    rows = []
    list_of_Inputs = []

    for csv_file in csv_files:
       rows.extend(read_csv_file(csv_file, folder_path))
       req_body = create_json_objects(rows)
       operate_on_paymentVehicle_csv(logger,req_body, properties, list_of_Inputs)
       index = csv_file.find(".csv")
       create_csv_for_ingestion(logger,list_of_Inputs, properties["ingestion_input_filepath"]+csv_file[:index]+"_input_data.csv",cipherKey)
    logger.info("read_csv_call_bulk_API_Completed.........................................................................")
if __name__ == "__main__":
	main()