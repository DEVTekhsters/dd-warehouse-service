CREATE VIEW IF NOT EXISTS structured_view_data_element_types AS
SELECT
    c.detected_entity AS data_element_types,  -- The detected entity type (e.g., CVV, PERSON)
    COUNT(DISTINCT c.id) AS count_data_element_types,     -- Count of how many times the entity appears, distinct by c.id
    COUNT(DISTINCT c.column_name) AS total_columns,  -- Distinct columns where this entity appeared
    COUNT(DISTINCT t.id) AS total_tables,      -- Distinct tables where this entity appeared
    COUNT(DISTINCT splitByString('.', CAST(ds.fqnHash AS String))[2]) AS total_schemas,  -- Distinct schemas
    COUNT(DISTINCT splitByString('.', CAST(ds.fqnHash AS String))[3]) AS total_databases,  -- Distinct databases
    COUNT(DISTINCT e.serviceType) AS total_data_systems,  -- Distinct service types
    COUNT(DISTINCT d.region) AS total_locations  -- Distinct regions
FROM 
    column_ner_results c
LEFT JOIN 
    table_entity t ON t.id = c.table_id
LEFT JOIN 
    dbservice_entity e ON e.nameHash = splitByString('.', CAST(t.fqnHash AS String))[1]
LEFT JOIN 
    database_schema_entity ds ON splitByString('.', CAST(ds.fqnHash AS String))[1] = e.nameHash 
LEFT JOIN 
    dbservice_entity_meta_info d ON e.id = CASE 
                                              WHEN position(d.dbservice_entity_id, '.') > 0 
                                              THEN substring(d.dbservice_entity_id, 1, position(d.dbservice_entity_id, '.') - 1)
                                              ELSE d.dbservice_entity_id
                                            END
GROUP BY 
    c.detected_entity;