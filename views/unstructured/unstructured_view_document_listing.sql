CREATE VIEW IF NOT EXISTS unstructured_view_document_listing AS
SELECT
    file_name AS document_name,
    file_type AS document_type,
    JSONExtractRaw(json) AS entity,
    file_size / 1024 AS document_size_kb,
    region AS location,
    source AS data_source
FROM ner_unstructured_data;
