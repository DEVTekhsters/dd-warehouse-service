CREATE TABLE IF NOT EXISTS column_ner_results (
    id UUID DEFAULT generateUUIDv4(),
    table_id String,
    column_name String,
    json String,
    detected_entity String,
    data_element String,
    false_positive String,
    false_postive_note String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id;
