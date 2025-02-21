CREATE VIEW IF NOT EXISTS structured_view_data_locations AS 
SELECT
    d.region as location,
    COUNT(DISTINCT db.serviceType) AS total_data_systems,
    COUNT(DISTINCT t.id) AS total_tables,
    COUNT(DISTINCT arrayElement(splitByString('.', ds.fqnHash), 2)) AS total_schemas,
    COUNT(DISTINCT arrayElement(splitByString('.', ds.fqnHash), 3)) AS total_databases,
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
    database_schema_entity ds
    ON arrayElement(splitByString('.', ds.fqnHash), 1) = db.nameHash
JOIN
    table_entity t
    ON arrayElement(splitByString('.', t.fqnHash), 1) = arrayElement(splitByString('.', ds.fqnHash), 1)
JOIN
    column_ner_results c
    ON c.table_id = t.id
GROUP BY
    d.region;
