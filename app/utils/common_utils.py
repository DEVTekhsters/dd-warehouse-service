
import logging
from client_connect import Connection

# Setup logging
logger = logging.getLogger(__name__)

class BaseFileProcessor:

    def get_clickhouse_client(self):
        """Returns a ClickHouse client instance."""
        return Connection.client

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
