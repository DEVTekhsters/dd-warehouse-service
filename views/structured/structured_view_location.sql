CREATE VIEW IF NOT EXISTS structured_view_location AS
SELECT 
    COUNT(DISTINCT region) AS location
FROM
    dbservice_entity_meta_info;
