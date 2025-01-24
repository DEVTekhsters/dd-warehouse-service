CREATE VIEW IF NOT EXISTS unstructured_view_data_sensitivity AS 
SELECT 
    data_sensitivity,
    COUNT(data_sensitivity)
FROM 
    ner_unstructured_data
GROUP BY 
    data_sensitivity;
