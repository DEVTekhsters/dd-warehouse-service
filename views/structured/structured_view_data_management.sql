CREATE VIEW IF NOT EXISTS structured_view_data_management AS 
SELECT 
    COUNT(DISTINCT serviceType) AS total_services,
    COUNT(DISTINCT updatedBy) AS total_system_owner,
    COUNT(DISTINCT splitByString('.', fqnHash)[3]) AS total_databases,
    COUNT(DISTINCT splitByString('.', fqnHash)[2]) AS total_schemas
FROM 
    database_schema_entity ds
JOIN 
    dbservice_entity d
    ON splitByString('.', fqnHash)[1] = d.nameHash;
