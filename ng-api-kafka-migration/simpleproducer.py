import logging
from utils import Utils
from config import *
import sys
if __name__ == '__main__':
    print("Starting script %s for %s" % (sys.argv[0],sys.argv[1]))
    entityname=sys.argv[1]
    properties = getconfig(filename="properties.ini", section=entityname)
    logging.basicConfig(
        filename=properties["ref_log_file_location"],
        level=logging.DEBUG,
        format="%(asctime)s:%(levelname)s:%(message)s"
    )
    logger = logging.getLogger(entityname)
    commonfunctions = Utils()
    commonfunctions.set_logging(entityname)
    logger.info("%s Kafka migration job is starting." % (entityname))
    #commonfunctions.fetch_paginated_records(properties["reference_table_name"], int(properties["per_page_records"]),properties["ref_data_folder"],properties["dbconfig_name"],properties["ref_column_names"])
    #commonfunctions.push_to_kafka(properties["ref_data_folder"],properties["kafka_brokers"],properties["kafka_topic"])

