CREATE VIEW IF NOT EXISTS structured_view_data_sensitivity AS 
SELECT 
    data_sensitivity,
    COUNT(data_sensitivity)
FROM 
    column_ner_results
GROUP BY 
    data_sensitivity;
