CREATE VIEW IF NOT EXISTS unstructured_view_data_element_listing AS
SELECT
     DISTINCT detected_entity AS data_element_type,
    COUNT(detected_entity) AS data_elements,
    COUNT(DISTINCT file_name) AS documents,
    COUNT(DISTINCT source) AS data_systems,
    COUNT(DISTINCT region) AS locations
FROM ner_unstructured_data
GROUP BY detected_entity;
