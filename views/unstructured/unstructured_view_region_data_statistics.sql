CREATE VIEW IF NOT EXISTS unstructured_view_region_data_statistics AS
SELECT
    region AS data_system_location_name,
    COUNT(DISTINCT file_name) AS document_count,
    COUNT(DISTINCT file_type) AS document_type_count,
    COUNT(DISTINCT source) AS data_system_count
FROM ner_unstructured_data
GROUP BY region;
