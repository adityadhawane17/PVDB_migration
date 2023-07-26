import os
import sys
from config import *
import psycopg2
import logging as lg
import csv
import json
from bson.json_util import default
from confluent_kafka import Producer
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

    def csv_export(self,viewname,limit,offset,outputfolder,dbconfigname,columnnames):
        conn = None
        query = "SELECT {columnnames} FROM {viewname} limit {limit} offset {offset}".format(columnnames=columnnames,viewname=viewname,limit=limit,offset=offset)
        count_query= "SELECT count(*) FROM {viewname} limit {limit} offset {offset}".format(viewname=viewname,limit=limit,offset=offset)
        self.logging.info("Running query {query}".format(query=query))
        output_file = "{outputfolder}{viewname}_{offset}.csv".format(outputfolder=outputfolder,viewname=viewname,offset=offset)
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

    def create_kafka_payload(self, csvrow):
        self.logging.info("Creating kafka payload")
        payload={}
        payload["type"]="MIGRATED"
        payload["entityId"]=csvrow[0]
        payload["entityType"]="Member"
        payload["sourceSystem"]="api-migration-service"
        payload["sourcePayload"]={}
        payload["sourcePayload"]["new_id"]=csvrow[0]
        payload["sourcePayload"]["old_id"]=int(csvrow[1])
        return payload

    def kafka_produce(self,payload,broker,topic):
        conf = {'bootstrap.servers': broker}
        producer = Producer(**conf)

        encoded_record = json.dumps(payload, default=default).encode('utf-8')

        def delivery_callback(err, msg):
            if err:
                self.logging.error('%% Message failed delivery: %s\n' % err)
                sys.stderr.write('%% Message failed delivery: %s\n' % err)
                sys.exit(1)
            else:
                self.logging.info('%% Message delivered to %s [%d] @ %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))
                sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))
        try:
            producer.produce(topic, encoded_record, callback=delivery_callback)
        except BufferError:
            self.logging.error('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(producer))
            sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                             len(producer))
        producer.poll(0)
        self.logging.info('%% Waiting for %d deliveries\n' % len(producer))
        sys.stderr.write('%% Waiting for %d deliveries\n' % len(producer))
        producer.flush()
        sys.exit(0)

    def push_to_kafka(self,inputfolder,broker,topic):
        try:
            for filename in os.listdir(inputfolder):
                try:
                    filepath = "{inputfolder}{filename}".format(inputfolder=inputfolder,filename=filename)
                    self.logging.info("Kafka Producing file {filepath}".format(filepath=filepath))
                    with open(filepath,"r") as csv_file:
                        csv_reader = csv.reader(csv_file, delimiter=',')
                        line_count = 0
                        for row in csv_reader:
                            if line_count == 0:
                                self.logging.info(f'Column names are {", ".join(row)}')
                                line_count += 1
                            else:
                                payload = self.create_kafka_payload(row)
                                print(payload)
                                self.kafka_produce(payload,broker,topic)
                                line_count += 1
                        self.logging.info(f'Processed {line_count} lines.')
                except Exception as e:
                    self.logging.error("File {filepath} can not be loaded. Please check.".format(filepath=filepath))
                    self.logging.error("Error: {}".format(str(e)))
                    sys.exit(1)
        except Exception as e:
            self.logging.error("Can not Push Messages")
            self.logging.error("Error: {}".format(str(e)))
            sys.exit(1)
        finally:
            self.logging.info("Kafka Jobs are terminating.")

    def fetch_paginated_records(self,sourcename, per_page_records,outputfolder,dbconfigname,columnnames):
        self.logging.info("Start fetching paginated records for {sourcename}".format(sourcename=sourcename))
        count = self.get_count(sourcename,dbconfigname)
        number_of_pages = (count // per_page_records)
        if (count % per_page_records) != 0 :
            number_of_pages = number_of_pages + 1
        self.logging.info("Number of files to be created is {number_of_pages}".format(number_of_pages=number_of_pages))
        offset = 0
        while (offset <= count):
            self.logging.info("Performing CSV Export for {sourcename} from offset {offset}".format(sourcename=sourcename,offset=offset))
            self.csv_export(sourcename,per_page_records,offset,outputfolder,dbconfigname,columnnames)
            offset = offset + per_page_records