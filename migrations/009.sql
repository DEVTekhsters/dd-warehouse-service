CREATE TABLE IF NOT EXISTS data_element
(   id UUID DEFAULT generateUUIDv4(),
    parameter_name String,
    parameter_value String,
    parameter_sensitivity String
)
ENGINE = MergeTree()
ORDER BY id;
