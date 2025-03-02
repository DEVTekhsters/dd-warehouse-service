CREATE VIEW IF NOT EXISTS structured_view_meta_data AS 
SELECT 
    COUNT(DISTINCT column_name) AS total_columns,
    COUNT(DISTINCT table_names) AS total_tables,
    COUNT(DISTINCT database_names) AS total_databases,
    COUNT(DISTINCT service_names) AS total_services,
    COUNT(DISTINCT data_element) AS total_data_elements,
    COUNT(DISTINCT detected_entity) AS total_detected_entity,
FROM 
    structured_view_data_final;
