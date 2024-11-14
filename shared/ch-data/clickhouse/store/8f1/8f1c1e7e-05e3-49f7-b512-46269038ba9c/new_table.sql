ATTACH TABLE _ UUID '833f1b65-a6b6-4f05-9c59-2334416b43ff'
(
    `key` UInt32,
    `value` String,
    `metric` Float64
)
ENGINE = MergeTree
ORDER BY key
SETTINGS index_granularity = 8192
