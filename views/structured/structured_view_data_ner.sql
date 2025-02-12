CREATE VIEW IF NOT EXISTS structured_view_data_ner AS 
SELECT
    c.*, 
     d.parameter_sensitivity
FROM 
    column_ner_results c
LEFT JOIN 
    data_element d
    ON c.detected_entity = d.parameter_value;