structured_view_location = """
CREATE VIEW IF NOT EXISTS structured_view_location AS
SELECT 
    COUNT(DISTINCT region) AS location
FROM
    dbservice_entity_meta_info;
"""

structured_view_data_management = """
CREATE VIEW IF NOT EXISTS structured_view_data_management AS 
SELECT 
    COUNT(DISTINCT serviceType) AS total_services,
    COUNT(DISTINCT updatedBy) AS total_system_owner,
    COUNT(DISTINCT splitByString('.', entityFQNHash)[2]) AS total_databases,
    COUNT(DISTINCT splitByString('.', entityFQNHash)[3]) AS total_schemas
FROM 
    profiler_data_time_series p
JOIN 
    dbservice_entity d
    ON splitByString('.', entityFQNHash)[1] = d.nameHash;
"""
structured_view_data_details = """ 
CREATE VIEW IF NOT EXISTS structured_view_data_details AS 
SELECT 
    COUNT(de.parameter_name) AS total_data_elements,
    COUNT(DISTINCT c.detected_entity) AS total_data_element_type,
    COUNT(DISTINCT de.parameter_name) AS total_data_element_category
FROM 
    column_ner_results c
JOIN 
    data_element de
ON c.data_element = de.parameter_name;  

"""

structured_view_data_locations = """
CREATE VIEW IF NOT EXISTS structured_view_data_locations AS 
SELECT 
    d.region,
    COUNT(DISTINCT db.serviceType) AS total_data_systems,
    COUNT(DISTINCT arrayElement(splitByString('.', p.entityFQNHash), 4)) AS total_tables,
    COUNT(DISTINCT arrayElement(splitByString('.', p.entityFQNHash), 3)) AS total_schemas,
    COUNT(DISTINCT arrayElement(splitByString('.', p.entityFQNHash), 2)) AS total_databases,
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
"""

structured_view_data_element_categories = """ 
CREATE VIEW IF NOT EXISTS structured_view_data_element_categories AS
SELECT
    detected_entity,
    COUNT(*) AS data_element_category
FROM 
    column_ner_results
GROUP BY 
    detected_entity;
 """

structured_view_data_element_types = """ 
CREATE VIEW IF NOT EXISTS structured_view_data_element_types AS
SELECT
    c.detected_entity AS data_element_types,                  -- its total number of times it has appered 
    COUNT(*) AS count_data_element_types,
    COUNT(DISTINCT splitByString('.', CAST(p.entityFQNHash AS String))[5]) AS total_columns,
    COUNT(DISTINCT splitByString('.', CAST(p.entityFQNHash AS String))[4]) AS total_tables,
    COUNT(DISTINCT splitByString('.', CAST(p.entityFQNHash AS String))[3]) AS total_schemas,
    COUNT(DISTINCT splitByString('.', CAST(p.entityFQNHash AS String))[2]) AS total_databases,
    COUNT(DISTINCT e.serviceType) AS total_data_systems,
    COUNT(DISTINCT d.region) AS total_locations
FROM 
    column_ner_results c
LEFT JOIN 
    dbservice_entity_meta_info d ON d.dbservice_entity_id = c.table_id
LEFT JOIN 
    dbservice_entity e ON e.id = d.dbservice_entity_id
LEFT JOIN 
    profiler_data_time_series p ON e.nameHash = splitByString('.', CAST(p.entityFQNHash AS String))[1]
GROUP BY 
    c.detected_entity;

 """
structured_view_data_shankey = """
CREATE VIEW IF NOT EXISTS structured_view_data_shankey AS 
SELECT 
    d.region AS location,
    groupArrayDistinct(db.serviceType) AS data_systems,
    groupArrayDistinct(db.updatedBy) AS data_system_owners,
    groupArrayDistinct(s.name) AS databases,
    groupArrayDistinct(c.column_name) AS tags,
    groupArrayDistinct(c.detected_entity) AS pii
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
GROUP BY 
    d.region;

"""