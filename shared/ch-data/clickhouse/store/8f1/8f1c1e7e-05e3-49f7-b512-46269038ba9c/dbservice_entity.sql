ATTACH TABLE _ UUID 'd17d0fc3-76a5-4e9c-a2f4-dd2d0ab41afe'
(
    `id` String,
    `name` String,
    `serviceType` String,
    `json` String,
    `updatedAt` UInt64,
    `updatedBy` String,
    `deleted` UInt8,
    `nameHash` String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 8192
