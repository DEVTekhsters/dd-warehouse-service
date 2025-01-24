CREATE VIEW IF NOT EXISTS unstructured_view_data_management AS
SELECT
    COUNT(DISTINCT file_name) AS scanned_documents,
    SUM(file_size) / 1024 AS scanned_volume,
    COUNT(DISTINCT file_type) AS file_format_count
FROM ner_unstructured_data;
