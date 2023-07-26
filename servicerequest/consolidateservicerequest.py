from ConsolidateSrRecordsForBookEvent import ConsolidateSRBE
from utils import Utils
import logging
from config import *
from ConsolidateSrRecords import ConsolidateSR
if __name__ == '__main__':
    properties = getconfig(filename="properties.ini",section='servicerequest-db-migration')
    logging.basicConfig(
        filename=properties["servicerequest_load_log_file_location"],
        level=logging.DEBUG,
        format="%(asctime)s:%(levelname)s:%(message)s"
    )
    logger=logging.getLogger("consolidateservicerequest")
    commonfunctions = Utils()
    commonfunctions.set_logging("consolidateservicerequest")
    logger.info("Consolidateservicerequest migration job is starting.")
    # commonfunctions.csv_export("cb_event",int(properties["per_page_records"]),0,"sqlscripts/extract/sr/cb_event.sql",properties["servicerequest_other_data_folder"],properties["migration_dbconfig_name"],properties["stage_servicerequest_column_names"])
    # commonfunctions.csv_export("on_sale_reg_event",int(properties["per_page_records"]),0,"sqlscripts/extract/sr/on_sale_reg_event.sql",properties["servicerequest_other_data_folder"],properties["migration_dbconfig_name"],properties["stage_servicerequest_column_names"])
    # commonfunctions.csv_export("sr_voucher",int(properties["per_page_records"]),0,"sqlscripts/extract/sr/sr_voucher.sql",properties["servicerequest_other_data_folder"],properties["migration_dbconfig_name"],properties["stage_servicerequest_column_names"])
    # commonfunctions.csv_export("sr_member",int(properties["per_page_records"]),0,"sqlscripts/extract/sr/sr_member.sql",properties["servicerequest_other_data_folder"],properties["migration_dbconfig_name"],properties["stage_servicerequest_column_names"])
    # commonfunctions.csv_export("sr_membership",int(properties["per_page_records"]),0,"sqlscripts/extract/sr/sr_membership.sql",properties["servicerequest_other_data_folder"],properties["migration_dbconfig_name"],properties["stage_servicerequest_column_names"])
    # commonfunctions.csv_export("sr_country",int(properties["per_page_records"]),0,"sqlscripts/extract/sr/sr_country.sql",properties["servicerequest_other_data_folder"],properties["migration_dbconfig_name"],properties["stage_servicerequest_column_names"])
    # commonfunctions.csv_export("sr_pv",int(properties["per_page_records"]),0,"sqlscripts/extract/sr/sr_payment_vehicle.sql",properties["servicerequest_other_data_folder"],properties["migration_dbconfig_name"],properties["stage_servicerequest_column_names"])
    # commonfunctions.csv_export("book_event",int(properties["per_page_records"]),0,"sqlscripts/extract/sr/book_event.sql",properties["servicerequest_other_data_folder"],properties["migration_dbconfig_name"],properties["stage_servicerequest_column_names"])
    #
    # commonfunctions.fetch_paginated_records("ng_intermediate_data_store.stage_service_request","request_sysid", int(properties["per_page_records"]),
    #                                        properties["servicerequest_data_folder"],
    #                                        properties["migration_dbconfig_name"],
    #                                        properties["stage_servicerequest_column_names"],
    #                                        properties["stage_sr_extract_file_location"]);

    #next step
    # recordconsildation = ConsolidateSR()
    recordconsildation = ConsolidateSRBE()
    recordconsildation.set_logging("consolidateservicerequest")
    recordconsildation.read_sr_records(properties["servicerequest_data_folder"],properties["servicerequest_other_data_folder"],properties["servicerequest_data_output_folder"])
