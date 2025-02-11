CREATE VIEW IF NOT EXISTS structured_view_data_sensitivity AS 
SELECT
    de.parameter_sensitivity AS sensitivity,
    COUNT(DISTINCT c.detected_entity) AS sensitivity_count
FROM
    data_element de
JOIN
    column_ner_results c
    ON c.detected_entity = de.parameter_value
GROUP BY
    de.parameter_sensitivity;
