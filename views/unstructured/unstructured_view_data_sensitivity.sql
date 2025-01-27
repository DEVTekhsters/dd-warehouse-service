CREATE VIEW IF NOT EXISTS unstructured_view_data_sensitivity AS 
SELECT 
    data_sensitivity as sensitivity,
    COUNT(data_sensitivity) sensitivity_count
FROM 
    ner_unstructured_data
GROUP BY 
    data_sensitivity;
