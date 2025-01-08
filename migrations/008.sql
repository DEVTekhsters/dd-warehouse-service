CREATE TABLE IF NOT EXISTS instant_classifier
(
    id UUID DEFAULT generateUUIDv4(),           -- Unique identifier
    customer_id UInt32,                         -- Customer ID (integer)
    list_of_files Array(String),                -- List of file names (array of strings)
    json_output String,                         -- Scan output in JSON format
    created_at DateTime DEFAULT now(),          -- Creation timestamp
    updated_at DateTime DEFAULT now()           -- Update timestamp
)
ENGINE = MergeTree()
PARTITION BY customer_id                        -- Partition by customer_id for efficiency
ORDER BY id;                                    -- Order by id
 