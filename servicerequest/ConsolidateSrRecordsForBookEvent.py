import os
import csv
import json
import sys
from collections import OrderedDict
import copy
import logging as lg
import pytz
import datetime
import time

from mappings_book_event import MappingsBE


class ConsolidateSRBE:

    def set_logging(self,name):
        self.logging = lg.getLogger(name)

    def convert_to_utc(self,datestring):
        finaldate=None
        final_format = '%Y-%m-%dT%H:%M:%S.%fZ'
        for format in ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S']:
            try:
                pst = pytz.timezone('Etc/GMT').localize(datetime.datetime.strptime(datestring, format))
                finaldate = pst.astimezone(pytz.UTC).strftime(final_format)
                break
            except ValueError as e:
                pass
        return finaldate


    def load_additional_data_from_sr(self,sr_row):
        additionaldata=sr_row["additional_data"]
        #self.logging.info("Populating Level 0 additonal data without nested properties")
        for key, value in MappingsBE.level_zero_mapping.items():
            additionaldata[key]=sr_row.get(value)
        additionaldata["currency"] = "GBP"
        additionaldata["unitCurrency"] = "GBP"
        additionaldata["service"] = "Events: Booking Services"
        # if additionaldata["currency"] == "": additionaldata["currency"] = "GBP"
        # if additionaldata["unitCurrency"] == "": additionaldata["unitCurrency"] = "GBP"
        #self.logging.info("Populating Level 0 nested Json delivery address")
        for key, value in MappingsBE.deliveryAddress_mappings.items():
            additionaldata["deliveryAddress"][key]=sr_row.get(value)
        if additionaldata["deliveryAddress"]["addressLine3"] == "null": additionaldata["deliveryAddress"]["addressLine3"]=""
        #self.logging.info("Populating Level 0 nested Json details")
        for key, value in MappingsBE.level_zero_details_mappings.items():
            additionaldata["details"][key] = sr_row.get(value)

        # reg_evt_bkng_evnt_name
        if sr_row.get("request_type") == "OnSale Registration":
            additionaldata["details"]["eventName"] = sr_row.get("reg_evt_bkng_evnt_name")
            additionaldata["onSaleRegRequestId"] = ""
            additionaldata["deliveryName"] = sr_row.get("reg_evt_bkng_delivery_name")
            additionaldata["deliveryType"] = sr_row.get("reg_evt_bkng_ticket_delivery_type")

        #self.logging.info("Populating Level 0 nested Json details")
        for key, value in MappingsBE.seats_deatils_mappings.items():
            additionaldata["details"]["seats"][0][key] = sr_row.get(value)

        #Total Cashback calculations
        if sr_row.get("total_price_amt") and sr_row.get("total_price_after_cb_amt"):
            additionaldata["totalCashback"] = float(sr_row.get("total_price_amt")) - float(sr_row.get("total_price_after_cb_amt"))

        #Convert Boolean Values level 0 fields only
        for item in ["isLastCashbackBooking","preRegistrationFlag"]:
            if (additionaldata[item] == "0" or additionaldata[item] == 0 or additionaldata[item] == None  or additionaldata[item] == ""): additionaldata[item] = False
            else: additionaldata[item] = True

        if (additionaldata["isEligibleForCashback"] == "1" or additionaldata["isEligibleForCashback"] == 1): additionaldata["isEligibleForCashback"] = False
        else: additionaldata["isEligibleForCashback"] = True


        #Convert Date Fields to UTC format
        for item in MappingsBE.field_date_format_mappings:
            if additionaldata[item] != "": additionaldata[item] = self.convert_to_utc(additionaldata[item])

        # convert details datetime to UTC
        if additionaldata["details"]["dateTime"] != "": additionaldata["details"]["dateTime"] = self.convert_to_utc(
            additionaldata["details"]["dateTime"])


    def get_dict(self,file):
        self.logging.info("Getting Dict for {file}".format(file=file))
        filedict=None
        with open(file, 'r') as f:
            r = csv.reader(f)
            next(r) #skipping the header
            filedict = {row[0]: row[1:] for row in r}
        return filedict

    def read_sr_records(self,inputfolder,otherinputfolder,outputfolder):
        self.logging.info("Starting with creating JSON CSV")
        cbeventdict=self.get_dict('{otherinputfolder}book_event_0.csv'.format(otherinputfolder=otherinputfolder))
        # cbeventdict=self.get_dict('{otherinputfolder}cb_event_0.csv'.format(otherinputfolder=otherinputfolder))
        voucherdict=self.get_dict('{otherinputfolder}sr_voucher_0.csv'.format(otherinputfolder=otherinputfolder))
        countrydict=self.get_dict('{otherinputfolder}sr_country_0.csv'.format(otherinputfolder=otherinputfolder))
        pvdict=self.get_dict('{otherinputfolder}sr_pv_0.csv'.format(otherinputfolder=otherinputfolder))
        memberdict=self.get_dict('{otherinputfolder}sr_member_0.csv'.format(otherinputfolder=otherinputfolder))
        membershipdict=self.get_dict('{otherinputfolder}sr_membership_0.csv'.format(otherinputfolder=otherinputfolder))
        onsaledict = self.get_dict('{otherinputfolder}on_sale_reg_event_0.csv'.format(otherinputfolder=otherinputfolder))

        for filename in os.listdir(inputfolder):
            try:
                allrows=[]
                filepath = "{inputfolder}{filename}".format(inputfolder=inputfolder, filename=filename)
                outputfilename="{outputfolder}op_{filename}".format(outputfolder=outputfolder,filename=filename)
                self.logging.info("Importing file {filepath}".format(filepath=filepath))
                with open(filepath, "r") as f:
                    fadd = open("sqlscripts/extract/sr_json_sample_v1", "r")
                    additionalData = json.load(fadd)
                    fadd.close()
                    fadd=None
                    reader = csv.DictReader(f, delimiter=',')
                    fieldnames=["request_sysid","request_type","request_subtype","description","summary","status","substatus","add_username",
                            "add_date","update_username","update_date","member","owner","additional_data"]
               
                    for row in reader:
                        #populate additional data base elements that do not require joins
                        row["additional_data"] = copy.deepcopy(additionalData)
                        self.load_additional_data_from_sr(row)
                        row["owner"] = json.dumps({"id":row["owner_username"]})
                        row["member"] = {"id": "","membershipId":""}

                        #populate additional data with joined columns
                        # payment vehicle
                        pv = pvdict.get(row.get("protected_card_sysid"))
                        if pv:
                            #self.logging.info("PV Match")
                            pv_data = json.loads(pv[0])
                            for item in pv_data:
                                item["amount"] = row.get("total_price_amt")
                            row["additional_data"]["payments"] = pv_data

                        #self.logging.info("Populating CB Event by Joining CSV from {otherinputfolder} folder".format(otherinputfolder=otherinputfolder))
                        cbevent=cbeventdict.get(row.get("discnt_evnt_bkng_sysid"))
                        if cbevent :
                            # self.logging.info("CB Event Match")
                            row["additional_data"]["units"] = json.loads(cbevent[0])


                        # self.logging.info("Populating Onsale Event by Joining CSV from {otherinputfolder} folder".format(otherinputfolder=otherinputfolder))
                        if row.get("request_type") == "OnSale Registration":
                            onsalevent = onsaledict.get(row.get("onsale_reg_evt_bkng_id"))
                            if onsalevent:
                                # self.logging.info("CB Event Match")
                                row["additional_data"]["units"] = json.loads(onsalevent[0])
                        #self.logging.info("Populating Voucher by Joining CSV from {otherinputfolder} folder".format(otherinputfolder=otherinputfolder))
                        voucher=voucherdict.get(row.get("voucher_header_id"))
                        if voucher :
                            #self.logging.info("Voucher Match")
                            # self.logging.info("PV Match")
                            voucher_data = json.loads(voucher[0])
                            for item in voucher_data:
                                if item["expiryDate"] != "":
                                    item["expiryDate"] = self.convert_to_utc(item["expiryDate"])
                            row["additional_data"]["voucherDetails"]=voucher_data
                        #populate country
                        #self.logging.info("Populating Country by Joining CSV from {otherinputfolder} folder".format(otherinputfolder=otherinputfolder))
                        country = countrydict.get(row.get("country"))
                        if country:
                            #self.logging.info("Country Match")
                            row["additional_data"]["deliveryAddress"]["countryName"] = country[0]
                            row["additional_data"]["deliveryAddress"]["countryISOCode"] = country[1]

                        # self.logging.info("Populating Sales Reg by Joining CSV from {otherinputfolder} folder".format(otherinputfolder=otherinputfolder))
                        member = memberdict.get(row.get("member_id"))
                        if member:
                            # customerEmail logic
                            row["additional_data"]["customerEmail"] = member[1]
                            # self.logging.info("Member Match")
                            row["member"]["id"] = member[0]

                        # self.logging.info("Populating Sales Reg by Joining CSV from {otherinputfolder} folder".format(otherinputfolder=otherinputfolder))
                        membership = membershipdict.get(row.get("membership_id"))
                        if membership:
                            #self.logging.info("Membership Match")
                            row["member"]["membershipId"] = membership[0]
                        #self.logging.info("Writing row to output file {outputfilename}".format(outputfilename=outputfilename))
                        row["member"] = json.dumps(row["member"])

                        #Preferences Population
                        preferences=[]
                        #ssr.preference_1
                        preferences.append({"order":1,"venue":"","timing":"","additionalInfo":{"notes":row.get("preference_1")}})

                        # ssr.preference_2
                        preferences.append({"order": 2, "venue": "", "timing": "",
                                            "additionalInfo": {"notes": row.get("preference_2")}})

                        # ssr.preference_3
                        preferences.append({"order": 3, "venue": "", "timing": "",
                                            "additionalInfo": {"notes": row.get("preference_3")}})

                        # ssr.preference_4
                        preferences.append({"order": 4, "venue": "", "timing": "",
                                            "additionalInfo": {"notes": row.get("preference_4")}})

                        # ssr.preference_5
                        preferences.append({"order": 5, "venue": "", "timing": "",
                                            "additionalInfo": {"notes": row.get("preference_5")}})
                        row["additional_data"]["details"]["preferences"] = preferences
                        row["additional_data"] = json.dumps(row["additional_data"])
                        allrows.append(row)
                    reader=None
                with open(outputfilename, "w", newline='') as fop:
                    writer = csv.DictWriter(fop, fieldnames=fieldnames, extrasaction='ignore', delimiter=',')
                    writer.writeheader()  # writing header to Output files
                    writer.writerows(allrows)
                    allrows=None
                    writer=None
                    self.logging.info("Finished")
            
            except Exception as e:
                self.logging.error(e)