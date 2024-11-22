import logging
from pathlib import Path
from app.utils.ner_scanner.check_digit import CheckDigitScanner
from typing import Dict, List, Union

# Setup logging
logger = logging.getLogger(__name__)

class Verify_PII_Digit:
    def __init__(self):
        """
        Initializes the Verify_PII_Digit class with necessary settings.
        """
        self.check_digit = CheckDigitScanner()

        # Define the list of PII entities for validation (shared across all methods)
        self.PII_ENTITY = [
            "AADHAAR", "PAN", "CREDIT_CARD", "VISA_CARD", "MASTER_CARD",
            "AMEX_CARD", "RUPAY_CARD", "MAESTRO_CARD", "VOTERID",
            "BANK_ACCOUNT_NUMBER", "PHONE_NUMBER", "GST_NUMBER", "MAC_ADDRESS"
        ]

    def verify(self, result: Union[Dict, List], file_type: str) -> Union[None, str, Dict]:
        """
        Verifies and validates PII entities in the given result based on the file type.
        
        Args:
            result (Dict or List): The result data (either image or document data) to be verified.
            file_type (str): The file type, either "DOCX", "PDF", "TXT", or image formats like "JPG", "PNG".
        
        Returns:
            None or str: Returns an error message if the file type is invalid or another error occurs.
            or returns a dictionary with entity types and validation scores.
        """
        try:
            if file_type in ["DOCX", "PDF", "TXT"]:
                return self._verifying_result(result, is_image=False)  # Process document-based results
            elif file_type in ["JPEG", "JPG", "PNG"]:
                return self._verifying_result(result, is_image=True)  # Process image-based results
            else:
                return "Wrong type"  # Return an error message for unsupported file types

        except Exception as e:
            logger.error(f"Error occurred: {e}")
            return "Error during verification"

    def _verifying_result(self, result: Union[Dict, List], is_image: bool) -> Union[str, Dict]:
        """
        Verifies and validates PII entities detected in files or images.
        
        Args:
            result (Dict or List): The result dictionary containing entity detection data.
            is_image (bool): Flag to determine whether the input is an image or a file.
        
        Returns:
            str: Confirmation or error message if no entities found or error occurred.
            or returns a dictionary with entity types and validation scores.
        """
        try:
            result_dict: Dict[str, float] = {}

            # Extract entities based on whether it's a file or an image
            if is_image:
                image_data = result[0]  # Assuming the result is a list with a dictionary at index 0
                identifiers = image_data.get("identifiers", [])
                phone_numbers = image_data.get("phone_numbers", [])
                entity_lists = [identifiers, phone_numbers]
            else:
                # Process files (DOCX, PDF, TXT)
                entities = result.get("result", {}).get("entity_detected_spacy", [])[0].get("entities", {})
                entity_lists = [entities.get(entity, []) for entity in self.PII_ENTITY if entity in entities]

            if not entity_lists:
                logger.info("No entities detected.")
                return "No entities detected."

            # Validate the entities
            for entity_list in entity_lists:
                for value in entity_list:
                    check_entity_type, check_entity_type_score = self.check_digit.identify_entity(str(value))
                    result_dict[check_entity_type] = check_entity_type_score

            logger.info("Validation complete.")
            return result_dict

        except Exception as e:
            logger.error(f"Error occurred during PII verification: {e}")
            return "Error during verification"

