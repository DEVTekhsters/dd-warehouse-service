CREATE VIEW IF NOT EXISTS unstructured_view_data_sensitivity AS 
SELECT 
    de.parameter_sensitivity AS sensitivity,
    COUNT(n.detected_entity) AS sensitivity_count
FROM 
    ner_unstructured_data n
JOIN 
    data_element de ON n.detected_entity = de.parameter_value
GROUP BY 
    de.parameter_sensitivity;