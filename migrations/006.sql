CREATE TABLE IF NOT EXISTS dbservice_entity_meta_info (
    id UUID DEFAULT generateUUIDv4(),
    dbservice_entity_id String,
    dbservice_entity_name String,
    source String,
    region String,
) ENGINE = MergeTree()
ORDER BY id;
