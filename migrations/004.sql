CREATE TABLE IF NOT EXISTS profiler_data_time_series (
    entityFQNHash String,  -- Equivalent to VARCHAR(768)
    extension String,  -- Equivalent to VARCHAR(256)
    jsonSchema String,  -- Equivalent to VARCHAR(256)
    json String,  -- Storing full JSON as String
    operation String,  -- Extracted from JSON, equivalent to 'operation' in MySQL
    timestamp UInt64  -- Equivalent to BIGINT UNSIGNED, extracted from JSON
) ENGINE = MergeTree()
ORDER BY (entityFQNHash, extension, operation, timestamp)
SETTINGS index_granularity = 8192;