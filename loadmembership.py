from utils import Utils
import logging
from config import *
from validation import Validation

if __name__ == '__main__':
    properties = getconfig(filename="properties.ini",section='api-db-migration')
    logging.basicConfig(
        filename=properties["membership_load_log_file_location"],
        level=logging.DEBUG,
        format="%(asctime)s:%(levelname)s:%(message)s"
    )
    logger=logging.getLogger("loadmembership")
    commonfunctions = Utils()
    commonfunctions.set_logging("loadmembership")
    logger.info("Membership migration job is starting.")
    commonfunctions.insert_from_file("sqlscripts/transform/add_member_to_membership.sql",
                                     properties["membership_dbconfig_name"])
    count_check = bool(
        commonfunctions.select_from_file("sqlscripts/post-validation/validate_member_count_in_membership.sql",
                                         properties["membership_dbconfig_name"])[0])
    Validation.validate_true(count_check, "Count is mismatched. Load is not successfull.")