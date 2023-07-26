from utils import Utils
import logging
from config import *
import datetime
from validation import Validation
if __name__ == '__main__':
    properties = getconfig(filename="properties.ini",section='api-db-migration')
    logging.basicConfig(
        filename=properties["payment_vehicle_log_file_location"],
        level=logging.DEBUG,
        format="%(asctime)s:%(levelname)s:%(message)s"
    )
    logger=logging.getLogger("loadPV_data")
    commonfunctions = Utils()
    commonfunctions.set_logging("load_payment_vehicle_Starting.........................................................................")
    commonfunctions.generate_crypto_key()
    logger.info("Payment_Vehicle DB migration job is starting.")
    tenant_array_string = properties["tenant_array"].split(',')
    str_list = [str(i) for i in tenant_array_string]
    tenant_array = ', '.join(str_list)
    commonfunctions.fetch_paginated_records(properties["payment_vehicle_view_name"], int(properties["per_page_records"]),properties["payment_vehicle_data_folder"],properties["payment_vehicle_dbconfig_name"],properties["payment_vehicle_column_names"],str(properties["start"]),str(properties["end"]),tenant_array)