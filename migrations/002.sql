CREATE TABLE IF NOT EXISTS database_schema_entity (
    id String,  -- VARCHAR(36)
    json String,  -- Store JSON as String
    updatedAt UInt64,  -- BIGINT UNSIGNED
    updatedBy String,  -- VARCHAR(256)
    deleted UInt8,  -- TINYINT(1)
    fqnHash String,  -- VARCHAR(768)
    name String  -- VARCHAR(256)
) ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 8192;