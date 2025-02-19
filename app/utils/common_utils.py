
import logging
from client_connect import Connection
from app.constants.rename_pii import RENAME_PII

# Setup logging
logger = logging.getLogger(__name__)

class BaseFileProcessor:

    def get_clickhouse_client(self):
        """Returns a ClickHouse client instance."""
        return Connection.client
    
    def pii_filter(self, ner_results: dict, file_name: str) -> dict:
        """
        Filters the NER results, keeping only valid PII entities.
        """
        if not ner_results or not isinstance(ner_results, dict) or "detected_entities" not in ner_results:
            logger.info("No valid PII detected in pii_filter.")
            return None  

        try:
            client = self.get_clickhouse_client()
            query = "SELECT parameter_value FROM data_element;"
            result = client.query(query)

            # Extract values as a set for fast lookup
            valid_pii_entities = {row[0] for row in result.result_rows}

            logger.info(f"Valid PII Entities from DB: {valid_pii_entities}")
            renamed_filtered_entities = {}
            for entity, count in ner_results["detected_entities"].items():
                if entity in valid_pii_entities:
                    renamed_filtered_entities[entity] = count  # Keep the original entity if it's valid
                elif entity in RENAME_PII and RENAME_PII[entity] in valid_pii_entities:
                    renamed_key = RENAME_PII[entity]  # Get the renamed key
                    renamed_filtered_entities[renamed_key] = count  # Assign the count to the renamed key
                
                        
            logger.info(f"Filtered Entities: {renamed_filtered_entities}")

            # If no valid entities remain, discard the file
            if not renamed_filtered_entities:
                logger.info(f"No valid PII entities found in {file_name}. Removing file.")
                return None

            # Update ner_results with filtered entities
            ner_results["detected_entities"] = renamed_filtered_entities
            ner_results["highest_label"] = max(renamed_filtered_entities, key=renamed_filtered_entities.get)  # Recalculate highest label
            ner_results["confidence_score"] = round(
                max(renamed_filtered_entities.values()) / sum(renamed_filtered_entities.values()), 2
            )

            return ner_results

        except Exception as e:
            logger.error(f"Error in pii_filter for file {file_name}: {str(e)}")
            return None  

    async def fetch_data_element_category(self, detected_entity: str) -> str:
        """Fetches the data element category from ClickHouse for a given detected entity."""
        try:
            client = self.get_clickhouse_client()
            query = f"SELECT parameter_name FROM data_element WHERE parameter_value = '{detected_entity}';"
            result = client.query(query)

            if result.result_rows:
                category = result.result_rows[0][0]
                logger.info(f"Data element category for '{detected_entity}': {category}")
                return category
            else:
                return await self.handle_unknown_category(client, detected_entity)
        except Exception as e:
            logger.error(f"Error fetching data element category: {str(e)}")
            return "Error"

    async def handle_unknown_category(self, client, detected_entity: str) -> str:
        """Handles the case where the detected entity is not found in the data element category."""
        logger.info(f"No category found for '{detected_entity}'. Checking 'UNKNOWN' category.")
        check_query = f"SELECT parameter_value FROM data_element WHERE parameter_name = 'UNKNOWN' AND parameter_value = '{detected_entity}';"
        unknown_result = client.query(check_query)

        if unknown_result.result_rows:
            logger.info(f"'{detected_entity}' exists in 'UNKNOWN' category. No action needed.")
        else:
            insert_query = f"INSERT INTO data_element (parameter_name, parameter_value, parameter_sensitivity) VALUES ('UNKNOWN', '{detected_entity}', 'UNKNOWN');"
            client.command(insert_query)
            logger.info(f"Added '{detected_entity}' to 'UNKNOWN' category.")
        return "UNKNOWN"
