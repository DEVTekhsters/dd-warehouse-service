CREATE TABLE IF NOT EXISTS region_mapping (
    code String,
    continent String,
    country String
) ENGINE = MergeTree()
ORDER BY code;
