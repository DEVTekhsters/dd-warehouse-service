#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
Regex scanner for column names
"""
import re
from typing import Optional


from enum import Enum
from pydantic import BaseModel

PII="PII"
class TagType(Enum):
    SENSITIVE = "Sensitive"
    NONSENSITIVE = "NonSensitive"


class TagAndConfidence(BaseModel):
    tag_fqn: str
    confidence: float




sensitive_regex = {
    'PASSWORD': re.compile(r"^.*pass(word|_word|).*$", re.IGNORECASE),

    "SSN": re.compile("^.*(ssn|social).*$", re.IGNORECASE),
    "CREDIT_CARD": re.compile("^.*(credit).*(card).*$", re.IGNORECASE),
    "BANK_ACCOUNT": re.compile("^.*bank.*(acc|num).*$", re.IGNORECASE),
    "EMAIL_ADDRESS": re.compile("^.*(email|e-mail|mail).*$", re.IGNORECASE),
    "USER_NAME": re.compile("^.*(user|client|person).*(name).*$", re.IGNORECASE),
    "PERSON": re.compile(
        r"^.*(firstname|fname|lastname|lname|fullname|maidenname|_name|"
        r"nickname|name_suffix|name|person).*$",
        re.IGNORECASE,
    ),
    'USERNAME': re.compile(r"^.*user(id|name|_id|_name|).*$", re.IGNORECASE),
    'POBOX': re.compile(r"^.*(po_box|pobox|postbox).*$", re.IGNORECASE),
    'PAYMENTCARD': re.compile(
        r"^.*(credit_card|cc_number|cc_num|creditcard|"
        r"credit_card_num|creditcardnumber|card_number|cardnum).*$",
        re.IGNORECASE,
    ),
    'AADHAAR': re.compile(r"^.*(aadhaar|uidai|aadhaar_number|aadhaar_no).*$", re.IGNORECASE),
    'PAN': re.compile(r"^.*(pan|pan_number|pan_no).*$", re.IGNORECASE),
    'VOTERID': re.compile(r"^.*(voter_id|voterid|voter).*$", re.IGNORECASE),
    'DRIVERLICENSE': re.compile(r"^.*(driver_license|license_no|dl_number|driving_license).*$", re.IGNORECASE),
    'PAASPORT': re.compile(r"^.*(passport|passport_number|passport_no).*$", re.IGNORECASE),
    'IP_ADDRESS': re.compile(r"^.*(ip_address|ipaddress|ipaddr|ip_addr|ip).*$", re.IGNORECASE),
    'MAC_ADDRESS': re.compile(r"^.*(mac_address|macaddress|macaddr|mac_addr|mac).*$", re.IGNORECASE),
    'BANK_ACCOUNT_NUMBER': re.compile(r"^.*(bank_account_number|bankaccountnumber|bank_acc_number|bank_acc_no|bank_account|acct_number|acct_no|account_no|account_number|acc_no|acc_number).*$", re.IGNORECASE),
    'IFSC': re.compile(r"^.*(ifsc|ifsc_code|ifsc_number).*$", re.IGNORECASE),
    'RATION_CARD_NUMBER': re.compile(r"^.*(ration_card_number|rationcardnumber|ration_card|rationcard|ration_no|rationnumber|rationcard_no|rationcard_number).*$", re.IGNORECASE),
    'VEHICLE_IDENTIFICATION_NUMBER': re.compile(r"^.*(vehicle|vehicle_id|vehicle_id_number|vehicle_identification_number|vehicle_number|vin|vin_number|veh_id|veh_identification|veh_number).*$", re.IGNORECASE),
    'UPI_ID': re.compile(r"^.*(upi_id|upiid|upi_id_number|upiidnumber|upi|upi_number).*$", re.IGNORECASE),
    'GST_NUMBER': re.compile(r"^.*(gst_number|gstnumber|gst_no|gstno|gst).*$", re.IGNORECASE),

}
non_sensitive_regex = {
    "BIRTH_DATE": re.compile(
        "^.*(date_of_birth|dateofbirth|dob|"
        "birthday|date_of_death|dateofdeath|birth_date|birthdate).*$",
        re.IGNORECASE,
    ),
    "GENDER": re.compile("^.*(gender).*$", re.IGNORECASE),
    "NATIONALITY": re.compile("^.*(nationality|nation).*$", re.IGNORECASE),
    "ADDRESS": re.compile(
        "^.*(address|city|state|county|country|"
        "zipcode|zip|postal|zone|borough|locality|district|area|road|street|lane).*$",
        re.IGNORECASE,
    ),
    "PHONE_NUMBER": re.compile(r"^.*(phone|mobile|contact|telephone).*$", re.IGNORECASE),
    'ZIPCODE': re.compile(
        r"^.*(zipcode|zip_code|postal|postal_code|zip|pincode|pin_code).*$",
        re.IGNORECASE,
    ),
}



