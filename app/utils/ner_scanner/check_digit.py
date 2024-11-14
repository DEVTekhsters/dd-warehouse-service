import re
from stdnum.in_ import aadhaar, epic, gstin, pan, vid
from stdnum import luhn  # Import Luhn validation from stdnum

class CheckDigitScanner:
    def __init__(self):

        # Regex pattern for Bank Account Number
        self.bank_account_number_regex = re.compile(r"^\d{9,18}$")
        self.phone_number_regex = re.compile(r'^(?:\+|0{0,2})91[\s-]?([789]\d{9}|(\d[ -]?){10}\d)$')

        # Define scores for each entity type
        self.scores = {
            "AADHAAR": 0.9,
            "VOTERID": 0.9,
            "GST_NUMBER": 0.9,
            "PAN": 0.9,
            "VID": 0.9,
            "BANK_CARD": 0.9,
            "BANK_ACCOUNT_NUMBER": 0.8,
            "PHONE_NUMBER": 0.85,
        }

    def verify_aadhaar(self, number: str) -> bool:
        try:
            return aadhaar.is_valid(number)
        except ValueError:
            return False

    def verify_voter_id(self, number: str) -> bool:
        try:
            return epic.is_valid(number)
        except ValueError:
            return False

    def verify_gst(self, number: str) -> bool:
        try:
            return gstin.is_valid(number)
        except ValueError:
            return False

    def verify_pan(self, number: str) -> bool:
        try:
            return pan.is_valid(number)
        except ValueError:
            return False

    def verify_vid(self, number: str) -> bool:
        try:
            return vid.is_valid(number)
        except ValueError:
            return False

    def verify_bank_card(self, number: str) -> bool:
        """
        Validate credit card numbers using Luhn algorithm only.
        """
        number = re.sub(r'[\s-]', '', number)  # Remove spaces and dashes
        try:
            number = int(number)
            res = luhn.validate(number)
            return res
        except:
            return False



    def verify_bank_account_number(self, number: str) -> bool:
        return bool(self.bank_account_number_regex.match(number))

    def verify_phone_number(self, number: str) -> bool:
        return bool(self.phone_number_regex.match(number))

    def identify_entity(self, text: str) -> str:
        """
        Identify the entity type of the input text based on validation.
        """
        validation_methods = {
            "AADHAAR": self.verify_aadhaar,
            "VOTERID": self.verify_voter_id,
            "GST_NUMBER": self.verify_gst,
            "PAN": self.verify_pan,
            "VID": self.verify_vid,
            "PHONE_NUMBER": self.verify_phone_number,
            "BANK_CARD": self.verify_bank_card,
            "BANK_ACCOUNT_NUMBER": self.verify_bank_account_number,
        }

        # Check against specific entity types first
        for entity_type, verify_method in validation_methods.items():
            if verify_method(text):
                return entity_type, self.scores.get(entity_type, 0)

        # Check for bank cards after other validations
        if self.verify_bank_card(text):
            return "BANK_CARD", self.scores.get("BANK_CARD", 0)

        return None, 0

 