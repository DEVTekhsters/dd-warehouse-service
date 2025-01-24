CREATE VIEW IF NOT EXISTS structured_view_data_management AS 
SELECT 
    COUNT(DISTINCT serviceType) AS total_services,
    COUNT(DISTINCT updatedBy) AS total_system_owner,
    COUNT(DISTINCT splitByString('.', entityFQNHash)[3]) AS total_databases,
    COUNT(DISTINCT splitByString('.', entityFQNHash)[2]) AS total_schemas
FROM 
    profiler_data_time_series p
JOIN 
    dbservice_entity d
    ON splitByString('.', entityFQNHash)[1] = d.nameHash;
