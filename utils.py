import os
import sys
from config import *
import psycopg2
import logging as lg
from validation import Validation
from cryptography.fernet import Fernet
from fernetUtils import  FernetSingleton
class Utils:
    global_cipher_var = ""
    def set_logging(self,name):
        self.logging = lg.getLogger(name)

    def generate_crypto_key(self):
        key = Fernet.generate_key()
        with open("fernet_key.txt", "wb") as f:
            f.write(key)



    def get_count(self,tablename,dbconfigname,startDate,endDate,tenant_array):
        self.logging.info("Getting the count for {tablename}".format(tablename=tablename))
        conn = None
        query = "SELECT count(*) FROM {tablename} where created_on BETWEEN '{startDate}' AND '{endDate}' AND tenant_id IN ({tenants})".format(tablename=tablename,startDate=startDate,endDate=endDate,tenants=tenant_array)
        print(query)
        try:
            params=dbconfig(filename="database.ini",section=dbconfigname)
            conn = psycopg2.connect(**params)
            db_cursor = conn.cursor()
            db_cursor.execute(query)
            row = db_cursor.fetchone()
            db_cursor.close()
            #print("Count is fetched without error.")
            self.logging.info("Count is fetched without error.")
            return row[0]
        except (Exception, psycopg2.DatabaseError) as error:
            self.logging.error("Error fetching count for {tablename}".format(tablename=tablename))
            self.logging.error(error)
        except psycopg2.Error as e:
            self.logging.error("Error fetching count for {tablename}".format(tablename=tablename))
            t_message = "Error: " + e
            self.logging.error(t_message)
        finally:
            if conn is not None:
                #print("CLosing the DB connection.")
                self.logging.info("CLosing the DB connection.")
                conn.close()
    def cryptography_handler(value, isEncrypt):
        size = os.path.getsize("fernet_key.txt")
        with open("fernet_key.txt", "rb") as f:
            key = f.read(size).strip()
        #cipher = FernetSingleton(key).fernet
        cipher = Fernet(key)
        if(isEncrypt):
            return cipher.encrypt(value.encode())
        else:
            return cipher.decrypt(value).decode()

    def generate_csv_from_bulkData(self,tablename,dbconfigname,data_vault_id,viewname,offset,outputfolder):
            self.logging.info("Getting the count for {tablename}".format(tablename=tablename))
            conn = None
            query = "SELECT payment_vehicle_id, tenant_id FROM {} WHERE data_vault_id = {}".format(tablename, data_vault_id)
            output_file = "{outputfolder}{viewname}_{offset}.csv".format(outputfolder=outputfolder,viewname=viewname,offset=offset)
            try:
                params=dbconfig(filename="database.ini",section=dbconfigname)
                conn = psycopg2.connect(**params)
                db_cursor = conn.cursor()
                db_cursor.execute(query)
                row = db_cursor.fetchone()
                db_cursor.close()
                print("Count is fetched without error.")
                self.logging.info("Count is fetched without error.")
                return row[0]
            except (Exception, psycopg2.DatabaseError) as error:
                self.logging.error("Error fetching count for {tablename}".format(tablename=tablename))
                self.logging.error(error)
            except psycopg2.Error as e:
                self.logging.error("Error fetching count for {tablename}".format(tablename=tablename))
                t_message = "Error: " + e
                self.logging.error(t_message)
            finally:
                if conn is not None:
                    print("CLosing the DB connection.")
                    self.logging.info("CLosing the DB connection.")
                    conn.close()

    def csv_export(self,viewname,limit,offset,outputfolder,dbconfigname,columnnames,startDate,endDate,tenant_array):
        conn = None
        query = "SELECT {columnnames} FROM {viewname} where created_on >= '{startDate}' AND created_on <= '{endDate}' AND tenant_id IN ({tenants}) limit {limit} offset {offset}".format(columnnames=columnnames,viewname=viewname,startDate=startDate,endDate=endDate,tenants=tenant_array,limit=limit,offset=offset)
        count_query= "SELECT count(*) FROM {viewname} where created_on >= '{startDate}' AND created_on <= '{endDate}' AND tenant_id IN ({tenants}) limit {limit} offset {offset}".format(viewname=viewname,startDate=startDate,endDate=endDate,tenants=tenant_array,limit=limit,offset=offset)
        self.logging.info("Running query {query}".format(query=query))
        output_file = "{outputfolder}{viewname}_{offset}.csv".format(outputfolder=outputfolder,viewname=viewname,offset=offset)
        self.logging.info("Output file for the output is {output_file}".format(output_file=output_file))
        try:
            params = dbconfig(filename="database.ini",section=dbconfigname)
            conn = psycopg2.connect(**params)
            db_cursor = conn.cursor()
            SQL_for_file_output = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)
            with open(output_file, 'w', encoding="utf-8") as f_output:
                db_cursor.copy_expert(SQL_for_file_output, f_output)
            Validation.validate_csv_data(output_file)
        except (Exception, psycopg2.DatabaseError) as error:
            self.logging.error("Can not proceed with the operation.")
            self.logging.error(error)
            sys.exit(1)
        except psycopg2.Error as e:
            self.logging.error("Can not proceeed with the operation.")
            t_message = "Error: " + e + "/n query we ran: " + query + "/n t_path_n_file: " + output_file
            self.logging.error(t_message)
            sys.exit(1)
        finally:
            if conn is not None:
                self.logging.info("CLosing the DB connection.")
                conn.close()
# here its taking data from csv &
    def csv_import(self,tablename,inputfolder,columnnames,dbconfigname):
        conn = None
        try:
            params = dbconfig(filename="database.ini",section=dbconfigname)
            conn = psycopg2.connect(**params)
            for filename in os.listdir(inputfolder):
                try:
                    filepath = "{inputfolder}{filename}".format(inputfolder=inputfolder,filename=filename)
                    self.logging.info("Importing file {filepath}".format(filepath=filepath))
                    f = open(filepath, "r")
                    db_cursor = conn.cursor()
                    db_cursor.copy_expert("copy {} ({}) from STDIN CSV HEADER QUOTE '\"'".format(tablename,columnnames), f)
                    db_cursor.execute("commit;")
                    self.logging.info("Imported data into {}".format(tablename))
                except Exception as e:
                    self.logging.error("File {filepath} can not be loaded. Please check. Moving ahead with other remaining files.".format(filepath=filepath))
                    self.logging.error("Error: {}".format(str(e)))

        except Exception as e:
            self.logging.error("Can not import CSVs.")
            self.logging.error("Error: {}".format(str(e)))
            sys.exit(1)
        finally:
            if conn is not None:
                self.logging.info("DB connection closed.")
                conn.close()

    def fetch_paginated_records(self,sourcename, per_page_records,outputfolder,dbconfigname,columnnames,startDate,endDate,tenant_array):
        #print("Start fetching paginated records for {sourcename}".format(sourcename=sourcename))
        self.logging.info("Start fetching paginated records for {sourcename}".format(sourcename=sourcename))
        count = self.get_count(sourcename,dbconfigname,startDate,endDate,tenant_array)
        number_of_pages = (count // per_page_records)
        if (count % per_page_records) != 0 :
            number_of_pages = number_of_pages + 1
        self.logging.info("Number of files to be created is {number_of_pages}".format(number_of_pages=number_of_pages))
        offset = 0
        while (offset <= count):
            self.logging.info("Performing CSV Export for {sourcename} from offset {offset}".format(sourcename=sourcename,offset=offset))
            self.csv_export(sourcename,per_page_records,offset,outputfolder,dbconfigname,columnnames,startDate,endDate,tenant_array)
            offset = offset + per_page_records

    def insert_from_file(self,sqlfilepath,dbconfigname):
        self.logging.info("Running insert queries in {path}".format(path=sqlfilepath))
        conn = None
        try:
            params = dbconfig(filename="database.ini",section=dbconfigname)
            conn = psycopg2.connect(**params)
            db_cursor = conn.cursor()
            db_cursor.execute(open(sqlfilepath, "r").read())
            db_cursor.execute("commit;")
            self.logging.info("Finished running insert queries in {path}".format(path=sqlfilepath))
        except Exception as e:
            self.logging.error("Error: {}".format(str(e)))
            sys.exit(1)
        finally:
            if conn is not None:
                self.logging.info("DB connection closed.")
                conn.close()

    def select_from_file(self,sqlfilepath,dbconfigname):
        self.logging.info("Running select queries in {path}".format(path=sqlfilepath))
        conn = None
        try:
            params = dbconfig(filename="database.ini",section=dbconfigname)
            conn = psycopg2.connect(**params)
            db_cursor = conn.cursor()
            db_cursor.execute(open(sqlfilepath, "r").read())
            rows = db_cursor.fetchall()
            self.logging.info("Finished running select queries in {path}".format(path=sqlfilepath))
            return rows
        except Exception as e:
            self.logging.error("Error: {}".format(str(e)))
            sys.exit(1)
        finally:
            if conn is not None:
                self.logging.info("DB connection closed.")
                conn.close()
