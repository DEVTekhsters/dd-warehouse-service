CREATE VIEW IF NOT EXISTS unstructured_view_file_format_statistics AS
SELECT
    file_type,
    COUNT(DISTINCT file_name) AS total_documents,
    COUNT(DISTINCT source) AS data_system_count,
    COUNT(DISTINCT region) AS location_count
FROM ner_unstructured_data
GROUP BY file_type;
