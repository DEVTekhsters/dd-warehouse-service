ATTACH TABLE _ UUID '08c7710d-651a-4d8d-b946-ff9174653abf'
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
ORDER BY id
SETTINGS index_granularity = 8192
