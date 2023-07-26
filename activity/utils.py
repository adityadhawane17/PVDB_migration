import os
import sys
from config import *
import psycopg2
import logging as lg
import csv
from validation import Validation
class Utils:
    def set_logging(self,name):
        self.logging = lg.getLogger(name)

    def get_count(self,tablename,dbconfigname):
        self.logging.info("Getting the count for {tablename}".format(tablename=tablename))
        conn = None
        query = "SELECT count(*) FROM {}".format(tablename)
        try:
            params=dbconfig(filename="database.ini",section=dbconfigname)
            conn = psycopg2.connect(**params)
            db_cursor = conn.cursor()
            db_cursor.execute(query)
            row = db_cursor.fetchone()
            db_cursor.close()
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
                self.logging.info("CLosing the DB connection.")
                conn.close()

    def csv_export(self,viewname,limit,primary_key_id,sql_file_location,outputfolder,dbconfigname,columnnames):
        conn = None
        query = open(sql_file_location, "r").read().format(primary_key_id=primary_key_id,limit=limit)
        self.logging.info("Running query {query}".format(query=query))
        output_file = "{outputfolder}{viewname}_{primary_key_id}.csv".format(outputfolder=outputfolder,viewname=viewname,primary_key_id=primary_key_id)
        self.logging.info("Output file for the output is {output_file}".format(output_file=output_file))
        try:
            params = dbconfig(filename="database.ini",section=dbconfigname)
            conn = psycopg2.connect(**params)
            db_cursor = conn.cursor()
            SQL_for_file_output = "COPY ({0}) TO STDOUT WITH CSV HEADER".format(query)
            with open(output_file, 'w') as f_output:
                db_cursor.copy_expert(SQL_for_file_output, f_output)
            Validation.validate_csv_data(output_file)
        except (Exception, psycopg2.DatabaseError) as error:
            self.logging.error("Can not proceed with the operation.")
            self.logging.error(error)
        except psycopg2.Error as e:
            self.logging.error("Can not proceeed with the operation.")
            t_message = "Error: " + e + "/n query we ran: " + query + "/n t_path_n_file: " + output_file
            self.logging.error(t_message)
        finally:
            if conn is not None:
                self.logging.info("CLosing the DB connection.")
                conn.close()
        return output_file

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
                    db_cursor.copy_expert("copy {} ({}) from STDIN CSV HEADER QUOTE '\"' ".format(tablename,columnnames), f)
                    print("copy file done")
                    db_cursor.execute("commit;")
                    self.logging.info("Imported data into {}".format(tablename))
                except Exception as e:
                    print(e)
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

    def get_max_value(self, columnname, tablename, dbconfigname):
        self.logging.info("Getting the max value for {tablename}".format(tablename=tablename))
        conn = None
        query="select max({columnname}) from {tablename} where {columnname} < 1100000000;".format(columnname=columnname,tablename=tablename)
        try:
            params = dbconfig(filename="database.ini", section=dbconfigname)
            conn = psycopg2.connect(**params)
            db_cursor = conn.cursor()
            db_cursor.execute(query)
            row = db_cursor.fetchone()
            db_cursor.close()
            self.logging.info("Max value is fetched without error.")
            return row[0]
        except (Exception, psycopg2.DatabaseError) as error:
            self.logging.error("Error fetching max value for {tablename}".format(tablename=tablename))
            self.logging.error(error)
        except psycopg2.Error as e:
            self.logging.error("Error fetching max value for {tablename}".format(tablename=tablename))
            t_message = "Error: " + e
            self.logging.error(t_message)
        finally:
            if conn is not None:
                self.logging.info("CLosing the DB connection.")
                conn.close()

    def get_max_value_file(self,filelocation,limit):
        row_index=0
        primary_key_value=0
        max_serial = limit + 1

        with open(filelocation, "r") as file:
            reader = csv.reader(file, delimiter=',')
            for row in reader:
                if row_index == limit:
                    primary_key_value=int(row[0])
                row_index += 1
        return primary_key_value

    def fetch_paginated_records(self,sourcename, per_page_records,outputfolder,dbconfigname,columnnames,sql_file_location):
        self.logging.info("Start fetching paginated records for {sourcename}".format(sourcename=sourcename))
        primary_key_id=0
        max_primary_key_id=self.get_max_value("activity_sysid","ng_intermediate_data_store.stage_service_activity",dbconfigname)
        self.logging.info("Maximum value is {}".format(max_primary_key_id))
        self.logging.info("Number of records {per_page_records}".format(per_page_records=per_page_records))
        while (primary_key_id < 1100000000):
            self.logging.info("Performing CSV Export for {sourcename} for primary key {primary_key}".format(sourcename=sourcename,primary_key=primary_key_id))
            output_file_location = self.csv_export(sourcename,per_page_records,primary_key_id,sql_file_location,outputfolder,dbconfigname,columnnames)
            primary_key_id = self.get_max_value_file(output_file_location,per_page_records)
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
