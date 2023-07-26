from utils import Utils
import logging
from config import *
from validation import Validation
if __name__ == '__main__':
    properties = getconfig(filename="properties.ini",section='servicerequest-db-migration')
    logging.basicConfig(
        filename=properties["servicerequest_load_log_file_location"],
        level=logging.DEBUG,
        format="%(asctime)s:%(levelname)s:%(message)s"
    )
    logger=logging.getLogger("loadservicerequest")
    commonfunctions = Utils()
    commonfunctions.set_logging("loadservicerequest")
    logger.info("Servicerequest migration job is starting.")
    # commonfunctions.fetch_paginated_records("ng_intermediate_data_store.stage_service_request",
    #                                         int(properties["per_page_records"]),
    #                                         properties["servicerequest_data_folder"],
    #                                         properties["migration_dbconfig_name"],
    #                                         properties["stage_servicerequest_column_names"],
    #                                         "sqlscripts/extract/extract_member_details_for_tags.sql")
    commonfunctions.csv_import(properties["servicerequest_tmp_tbl_name"],properties["servicerequest_data_folder"],properties["column_names"],properties["servicerequest_dbconfig_name"])
    #commonfunctions.insert_from_file("sqlscripts/transform/add_servicerequest.sql",properties["servicerequest_dbconfig_name"])
