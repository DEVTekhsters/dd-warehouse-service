CREATE VIEW IF NOT EXISTS unstructured_view_document_listing AS
SELECT
    file_name AS document_name,                     
    file_type AS document_type,                  
    detected_entity AS entity,
    file_size / 1024 AS document_size_kb,  
    region AS location,                            
    source AS data_source,
    sub_service AS subservices
FROM ner_unstructured_data;