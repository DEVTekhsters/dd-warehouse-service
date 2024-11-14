ATTACH TABLE _ UUID '89a3d156-474c-4154-9244-588107a73ea3'
(
    `id` String,
    `json` String,
    `updatedAt` UInt64,
    `updatedBy` String,
    `deleted` UInt8,
    `fqnHash` String,
    `name` String
)
ENGINE = MergeTree
PRIMARY KEY id
ORDER BY id
SETTINGS index_granularity = 8192
