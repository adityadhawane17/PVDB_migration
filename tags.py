import os
import sys
import csv
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
from utils import Utils
import logging
from config import *
import json

def get_index_id(index_name,elasticsearch):
    all_indices = elasticsearch.indices.get_alias("*")
    for num, index in enumerate(all_indices):
        print(num)
        print(index)
        if index == index_name:
            return num

def read_docs(index,esclient):
    doc_count=0
    # declare a filter query dict object
    match_all = {
  "version": True,
  "size": 500,
  "sort": [
    {
      "_score": {
        "order": "desc"
      }
    }
  ],
  "_source": {
    "excludes": []
  },
  "stored_fields": [
    "*"
  ],
  "script_fields": {},
  "docvalue_fields": [
    {
      "field": "dateOfBirth",
      "format": "date_time"
    },
    {
      "field": "dateofBirth",
      "format": "date_time"
    }
  ],
  "query": {
    "bool": {
      "must": [],
      "filter": [
        {
          "match_all": {}
        }
      ],
      "should": [],
      "must_not": [
        {
          "exists": {
            "field": "clientId"
          }
        },
        {
          "match_phrase": {
            "status": {
              "query": "Cancelled"
            }
          }
        }
      ]
    }
  },
  "highlight": {
    "pre_tags": [
      "@kibana-highlighted-field@"
    ],
    "post_tags": [
      "@/kibana-highlighted-field@"
    ],
    "fields": {
      "*": {}
    },
    "fragment_size": 2147483647
  }
}
    # make a search() request to get all docs in the index
    resp = es.search(
        index="member",
        body=match_all,
        scroll='100s'  # length of time to keep search context
    )
    #print("total docs:", len(resp["hits"]["hits"]))
    # keep track of pass scroll _id
    old_scroll_id = resp['_scroll_id']
    # use a 'while' iterator to loop over document 'hits'
    doc_count = 0
    while len(resp['hits']['hits']):
        # make a request using the Scroll API
        resp = esclient.scroll(
            scroll_id=old_scroll_id,
            scroll='2s'  # length of time to keep search context
        )
        # check if there's a new scroll ID
        if old_scroll_id != resp['_scroll_id']:
            print("NEW SCROLL ID:", resp['_scroll_id'])

        # keep track of pass scroll _id
        old_scroll_id = resp['_scroll_id']
        # print the response results
        #print("\nresponse for index:", index)
        #print("_scroll_id:", resp['_scroll_id'])
        #print('response["hits"]["total"]["value"]:', resp["hits"]["total"]["value"])
        # iterate over the document hits for each 'scroll'
        for doc in resp['hits']['hits']:
            #print("\n", doc['_id'], doc['_source'])
            print(doc['_source']["id"])
            doc_count += 1
            #print("DOC COUNT:", doc_count)

    # print the total time and document count at the end

    print("\nTOTAL DOC COUNT:", doc_count)

def connect_elasticsearch():
    service = 'es'
    credentials = boto3.Session(
        aws_access_key_id=properties["aws_access_key_id"],
        aws_secret_access_key=properties["aws_secret_access_key"]
    ).get_credentials()
    awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, properties["region"], service,
                       session_token=credentials.token)
    es = Elasticsearch(
        hosts=[{'host': properties["host"], 'port': properties["port"]}],
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    #if es.ping():
        #print('Yay Connect')
    #else:
        #print('Awww it could not connect!')
    return es

def store_record(elastic_object, index_name, record):
    is_stored = True
    try:
        outcome = elastic_object.index(index=index_name, doc_type='_doc', body=record)
        print(outcome)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored

def get_member_json(member_record):
    return json.dumps({'firstName': member_record[0], 'lastName': member_record[1], 'isPrimary': member_record[2], 'id': member_record[3]})

if __name__ == '__main__':
    properties = getconfig(filename="properties.ini",section='member-tags')
    logging.basicConfig(
        filename=properties["member_tag_load_log_file_location"],
        level=logging.DEBUG,
        format="%(asctime)s:%(levelname)s:%(message)s"
    )
    logger=logging.getLogger("loadmembertag")
    commonfunctions = Utils()
    commonfunctions.set_logging("loadmembertag")
    logger.info("Member tag load job is starting.")
    logger.info("Extracting member details to CSV")

    es = connect_elasticsearch()
    read_docs("member",es)
    #commonfunctions.fetch_paginated_records(properties["member_table_name"], int(properties["per_page_records"]),properties["member_tag_data_folder"],properties["member_tag_dbconfig_name"],properties["member_tag_column_names"])

    #for filename in os.listdir(properties["member_tag_data_folder"]):
    #    filepath = "{inputfolder}{filename}".format(inputfolder=properties["member_tag_data_folder"], filename=filename)
    #    with open(filepath) as csv_file:
    #        csv_reader = csv.reader(csv_file, delimiter=',')
    #        line_count = 0
    #        for row in csv_reader:
    #            if line_count == 0:
    #                line_count += 1
    #            else:
    #                print(f'\t{row} .')
    #                print(get_member_json(row))
    #                store_record(es,"member",get_member_json(row))
    #                sys.exit(0)
    #                line_count += 1
    #        print(f'Processed {line_count} lines.')

