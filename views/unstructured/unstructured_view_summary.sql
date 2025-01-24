CREATE VIEW IF NOT EXISTS unstructured_view_summary AS
SELECT
    COUNT(DISTINCT source) AS total_data_system_count,
    COUNT(DISTINCT region) AS total_location_count
FROM ner_unstructured_data;
