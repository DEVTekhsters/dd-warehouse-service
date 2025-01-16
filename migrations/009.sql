CREATE TABLE IF NOT EXISTS data_element
(
    id Int32,
    parameter_name String,
    parameter_value Array(String)
)
ENGINE = MergeTree()
ORDER BY id;
