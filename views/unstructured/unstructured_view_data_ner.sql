CREATE VIEW IF NOT EXISTS unstructured_view_data_ner AS 
SELECT 
    Distinct c.*, 
    d.parameter_sensitivity
FROM 
    ner_unstructured_data c
LEFT JOIN 
    data_element d
    ON c.detected_entity = d.parameter_value;
