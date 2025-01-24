CREATE VIEW IF NOT EXISTS structured_view_data_locations AS 
SELECT 
    d.region as location,
    COUNT(DISTINCT db.serviceType) AS total_data_systems,
    COUNT(DISTINCT arrayElement(splitByString('.', p.entityFQNHash), 4)) AS total_tables,
    COUNT(DISTINCT arrayElement(splitByString('.', p.entityFQNHash), 2)) AS total_schemas,
    COUNT(DISTINCT arrayElement(splitByString('.', p.entityFQNHash), 3)) AS total_databases,
    COUNT(DISTINCT c.detected_entity) AS total_data_elements
FROM 
    dbservice_entity_meta_info d
JOIN
    dbservice_entity db 
    ON db.id = CASE 
                WHEN position(d.dbservice_entity_id, '.') > 0 
                THEN substring(d.dbservice_entity_id, 1, position(d.dbservice_entity_id, '.') - 1)
                ELSE d.dbservice_entity_id
              END
JOIN
    profiler_data_time_series p
    ON arrayElement(splitByString('.', p.entityFQNHash), 1) = db.nameHash
JOIN
    table_entity t
    ON arrayElement(splitByString('.', t.fqnHash), 1) = arrayElement(splitByString('.', p.entityFQNHash), 1)
JOIN
    column_ner_results c
    ON c.table_id = t.id
GROUP BY 
    d.region;
