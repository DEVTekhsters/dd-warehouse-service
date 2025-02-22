CREATE TABLE IF NOT EXISTS profiler_metadata (
    entityFQNHash String,
    rowCount String,
    timestamp UInt64,
    sizeInByte String,
    columnCount String,
    profileSample String,
    createDateTime UInt64,
    profileSampleType String
)
ENGINE = MergeTree()
ORDER BY timestamp;
