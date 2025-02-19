CREATE VIEW IF NOT EXISTS structured_view_data_final AS 
SELECT DISTINCT 
    c.id AS id,
    c.table_id AS table_id,
    c.column_name AS column_name,
    c.json AS json_data,
    c.detected_entity AS detected_entity,
    c.data_element AS data_element,
    c.created_at AS created_at,
    c.updated_at AS updated_at,
    de.parameter_sensitivity AS sensitivity,
    db.updatedBy AS data_system_owner,
    db.serviceType AS data_systems,
    db.name AS service_names,
    s.name AS database_names,
    t.name AS table_names 
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
    database_schema_entity s
    ON arrayElement(splitByString('.', s.fqnHash), 1) = db.nameHash
JOIN
    table_entity t
    ON arrayElement(splitByString('.', t.fqnHash), 1) = arrayElement(splitByString('.', s.fqnHash), 1)
JOIN
    column_ner_results c
    ON c.table_id = t.id
JOIN 
    data_element de  
    ON c.detected_entity = de.parameter_value;
