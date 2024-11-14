CREATE TABLE IF NOT EXISTS column_ner_results (
    id UUID DEFAULT generateUUIDv4(),
    table_id String,
    column_name String,
    json String,
    detected_entity String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;
