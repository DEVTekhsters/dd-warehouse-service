CREATE VIEW IF NOT EXISTS structured_view_data_element_categories AS
SELECT
    data_element,
    COUNT(*) AS data_element_category
FROM 
    column_ner_results
GROUP BY 
    data_element;
