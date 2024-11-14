CREATE TABLE IF NOT EXISTS table_entity (
    id String,  -- Equivalent to VARCHAR(36), storing 'id' from JSON
    json String,  -- Storing full JSON as String
    updatedAt UInt64,  -- Equivalent to BIGINT UNSIGNED, extracting 'updatedAt' from JSON
    updatedBy String,  -- Equivalent to VARCHAR(256), extracting 'updatedBy' from JSON
    deleted UInt8,  -- Equivalent to TINYINT(1), extracting 'deleted' from JSON
    fqnHash String,  -- Equivalent to VARCHAR(768), nullable
    name String  -- Equivalent to VARCHAR(256)
) ENGINE = MergeTree()
ORDER BY id
SETTINGS index_granularity = 8192;