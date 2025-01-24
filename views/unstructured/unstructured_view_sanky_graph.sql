CREATE VIEW IF NOT EXISTS unstructured_view_sanky_graph AS
SELECT DISTINCT
    region AS location,
    source AS data_service,
    sub_service AS sub_service,
    file_type AS file_format
FROM ner_unstructured_data;
