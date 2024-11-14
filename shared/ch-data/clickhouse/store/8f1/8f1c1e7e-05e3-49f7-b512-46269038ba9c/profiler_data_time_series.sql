ATTACH TABLE _ UUID '80245a2d-e1dd-4ffd-a733-2b603803f51d'
(
    `entityFQNHash` String,
    `extension` String,
    `jsonSchema` String,
    `json` String,
    `operation` String,
    `timestamp` UInt64
)
ENGINE = MergeTree
ORDER BY (entityFQNHash, extension, operation, timestamp)
SETTINGS index_granularity = 8192
