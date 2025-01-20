CREATE TABLE IF NOT EXISTS profiler_metadata (
    entityFQNHash String,
    rowCount Float64,
    timestamp UInt64,
    sizeInByte Float64,
    columnCount Float64,
    profileSample Float64,
    createDateTime UInt64,
    profileSampleType String
)
ENGINE = MergeTree()
ORDER BY timestamp;
