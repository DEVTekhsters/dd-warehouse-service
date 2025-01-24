CREATE VIEW IF NOT EXISTS structured_view_data_details AS 
SELECT 
    COUNT(c.id) AS total_data_elements,  -- Count of all data elements
    COUNT(DISTINCT c.detected_entity) AS total_data_element_type,  -- Count of distinct detected entity types
    (SELECT COUNT(DISTINCT parameter_name) FROM data_element) AS total_data_element_category  -- Count all distinct parameter names in data_element
FROM 
    column_ner_results c
LEFT JOIN 
    data_element de ON c.data_element = de.parameter_name
WHERE 
    c.data_element != 'NA';  -- Exclude rows where data_element is 'NA'
