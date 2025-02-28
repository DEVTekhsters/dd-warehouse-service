# from fastapi import UploadFile, HTTPException
# import logging
# import os
# import json
# from dotenv import load_dotenv
# from collections import defaultdict

# from pathlib import Path


# from client_connect import Connection
# from pii_scanner.scanner import PIIScanner
# from pii_scanner.constants.patterns_countries import Regions
# from app.utils.common_utils import BaseFileProcessor

# # Load environment variables
# load_dotenv()

# # Setup logging
# logger = logging.getLogger(__name__)

# # Define the temporary folder path for storing files
# TEMP_FOLDER = Path(__file__).resolve().parent.parent / 'utils/temp_files'
# if not TEMP_FOLDER.exists():
#     TEMP_FOLDER.mkdir(parents=True, exist_ok=True)

# class OmdFileProcesser(BaseFileProcessor):
#     def __init__(self):
#         self.pii_scanner = PIIScanner()
    
#     async def process_and_update_ner_results(self, table_id: str, file: UploadFile) -> bool:
#         """
#         Processes NER results for each column and updates the results in ClickHouse.
#         """
#         try:
#             temp_file_path = TEMP_FOLDER / file.filename.split("/")[-1]# Save the uploaded file
#             with temp_file_path.open("wb") as buffer:
#                 buffer.write(await file.read())

#             json_result = await self.pii_scanner.scan(file_path=str(temp_file_path), sample_size=5, region=Regions.IN)
            
#             if json_result:
#                 for column_name, column_data in json_result.items():
#                     print(column_data)
#                     if not column_data:
#                         logger.warning(f"No data for column: {column_name}")
#                         continue  # Skip processing this column

#                     entity_counts = defaultdict(int)
#                     total_entities = 0
#                     highest_label = "NA"
#                     column_result = column_data.get("results", [])
                    
#                     for result in column_result:
                        
#                         for entity in result.get("entity_detected", []):
#                             entity_type = entity.get("type")
#                             if entity_type:
#                                 entity_counts[entity_type] += 1
#                                 total_entities += 1

#                     if entity_counts:
#                         highest_label = max(entity_counts.items(), key=lambda x: x[1])[0]
#                         confidence_score = round(max(entity_counts.values()) / total_entities, 2)
#                         ner_results = {
#                             'highest_label': highest_label,
#                             'confidence_score': confidence_score,
#                             'detected_entities': dict(entity_counts)
#                         }
#                     else:
#                         ner_results = {'highest_label': "NA", 'confidence_score': 0.00, 'detected_entities': {"NA": 0}}
                    
#                     logger.info(f"NER RESULTS: {ner_results}")
                    
#                     if highest_label != "NA":
#                         updated_ner_results = self.pii_filter(ner_results, column_name)
#                         if updated_ner_results:
#                             detected_entity = updated_ner_results.get('highest_label', 'NA')
#                             data_element = await self.fetch_data_element_category(detected_entity)
#                             update_result = await self.update_entity_for_column(table_id, column_name, updated_ner_results, detected_entity, data_element)
#                             if not update_result:
#                                 logger.error(f"Failed to save NER results for table_id: {table_id}")
            
#             return True
#         except Exception as e:
#             logger.error(f"Error processing NER results: {str(e)}")
#             return False
#         finally:
#             if temp_file_path.exists():
#                 os.remove(temp_file_path)
    
#     async def update_entity_for_column(self, table_id: str, column_name: str, updated_ner_results, detected_entity: str, data_element) -> bool:
#         """
#         Updates the NER result for a given column in ClickHouse.
#         """
#         try:
#             client = self.get_clickhouse_client()
#             query = """
#                 INSERT INTO column_ner_results 
#                     (table_id, column_name, json, detected_entity, data_element)
#                 VALUES 
#                     (%(table_id)s, %(column_name)s, %(json)s, %(detected_entity)s, %(data_element)s)
#             """
#             params = {
#                 "table_id": table_id,
#                 "column_name": column_name,
#                 "json": json.dumps(updated_ner_results),
#                 "detected_entity": detected_entity,
#                 "data_element": data_element
#             }
#             client.command(query, params)
#             logger.info(f"Successfully inserted/updated NER result for table_id: '{table_id}', column: '{column_name}'")
#             return True
#         except Exception as e:
#             logger.error(f"Error inserting/updating data in ClickHouse: {str(e)}")
#             return False
