from utils import Utils
import logging
from config import *
from validation import Validation
if __name__ == '__main__':
    properties = getconfig(filename="properties.ini",section='servicerequest_activity-db-migration')
    logging.basicConfig(
        filename=properties["servicerequest_load_log_file_location"],
        level=logging.DEBUG,
        format="%(asctime)s:%(levelname)s:%(message)s"
    )
    logger=logging.getLogger("loadservicerequest")
    commonfunctions = Utils()
    commonfunctions.set_logging("loadservicerequest")
    logger.info("Servicerequest migration job is starting.")
    commonfunctions.fetch_paginated_records(properties["stage_servicerequest_activity_view_name"],int(properties["per_page_records"]),properties["servicerequest_activity_data_folder"],properties["migration_dbconfig_name"],properties["stage_servicerequest_activity_column_names"],properties["stage_sr_activity_extract_file_location"])

    commonfunctions.csv_import(properties["servicerequest_activity_tmp_tbl_name"],properties["servicerequest_activity_data_folder"],properties["column_names"],properties["servicerequest_activity_dbconfig_name"])
    # commonfunctions.insert_from_file("sqlscripts/transform/add_servicerequest_activity.sql",properties["servicerequest_activity_dbconfig_name"])
