unstructured_view_file_format_statistics = """
CREATE VIEW IF NOT EXISTS unstructured_view_file_format_statistics AS
SELECT
    file_type,
    COUNT(*) AS total_documents,              
    COUNT(DISTINCT source) AS data_system_count,
    COUNT(DISTINCT region) AS location_count   
FROM ner_unstructured_data
GROUP BY file_type;"""
 
unstructured_view_data_management = """
CREATE VIEW IF NOT EXISTS unstructured_view_data_management AS
SELECT
    COUNT(DISTINCT file_name) AS scanned_documents,     
    SUM(file_size) / (1024 * 1024) AS scanned_volume,   
    COUNT(DISTINCT file_type) AS file_format_count        
FROM ner_unstructured_data;"""

unstructured_view_region_data_statistics = """
CREATE VIEW IF NOT EXISTS unstructured_view_region_data_statistics AS
SELECT
    region AS data_system_location_name,         
    COUNT(DISTINCT file_name) AS document_count,  
    COUNT(DISTINCT file_type) AS document_type_count,
    COUNT(DISTINCT source) AS data_system_count  
FROM ner_unstructured_data
GROUP BY region;    """                              

unstructured_view_document_listing = """
CREATE VIEW IF NOT EXISTS unstructured_view_document_listing AS
SELECT
    file_name AS document_name,                     
    file_type AS document_type,                  
    JSONExtractArrayRaw(json, 'entity_types') AS entity,
    file_size / (1024 * 1024) AS document_size_mb,  
    region AS location,                            
    source AS data_source                           
FROM ner_unstructured_data;
 """

unstructured_view_summary = """
CREATE VIEW IF NOT EXISTS unstructured_view_summary AS
SELECT
    COUNT(DISTINCT source) AS total_data_system_count,       
    COUNT(DISTINCT file_name) AS total_document_count,       
    SUM(file_size) / (1024 * 1024) AS total_volume
FROM ner_unstructured_data;"""
 

unstructured_view_snaky_graph = """
CREATE VIEW IF NOT EXISTS unstructured_view_snaky_graph AS
SELECT
    region AS location,           
    source AS data_system,        
    file_type AS file_format_type  
FROM ner_unstructured_data;
 """

unstructured_view_data_details = """
CREATE VIEW IF NOT EXISTS unstructured_view_data_details AS
 
SELECT
   COUNT(ner.data_element) AS total_data_elements,
   COUNT(DISTINCT ner.detected_entity) AS total_data_element_type,
   COUNT(de.parameter_name) AS total_data_element_category
FROM
   ner_unstructured_data ner
LEFT JOIN
   data_element de
ON ner.data_element = de.parameter_name;"""
 
unstructured_view_data_element_listing ="""
CREATE VIEW IF NOT EXISTS unstructured_view_data_element_listing AS
SELECT
    detected_entity AS data_element_type,
    COUNT(detected_entity) AS data_elements,
    COUNT(file_name) AS documents,
    COUNT(source) AS data_systems,
    COUNT(region) AS locations
FROM
    ner_unstructured_data
GROUP BY
    detected_entity;"""



import clickhouse_connect

# Establish connection to ClickHouse
client = clickhouse_connect.get_client(
    host='148.113.6.50',
    port="8123",
    username='default',
    password='',
    database='default'
)
# Execute view creation queries
try:
    client.command(unstructured_view_file_format_statistics)
    client.command(unstructured_view_data_details)
    client.command(unstructured_view_document_listing)
    client.command(unstructured_view_data_element_listing)
    client.command(unstructured_view_data_management)
    client.command(unstructured_view_region_data_statistics)
    client.command(unstructured_view_snaky_graph)
    client.command(unstructured_view_summary)
    
    print("Views created for unstructured successfully.")
except Exception as e:
    print(f"An error occurred: {e}")