CREATE VIEW IF NOT EXISTS unstructured_view_data_details AS
SELECT
    COUNT(ner.detected_entity) AS total_data_elements,
    COUNT(DISTINCT ner.detected_entity) AS total_data_element_type,
    (SELECT COUNT(*) FROM data_element) AS total_data_element_category
FROM ner_unstructured_data ner
LEFT JOIN data_element de
ON ner.data_element = de.parameter_name;
