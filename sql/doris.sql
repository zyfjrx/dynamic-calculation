CREATE TABLE IF NOT EXISTS test_db.dws_tag_today
(
    `time` datetime,
    `byt_name` string,
    `topic` string,
    `value` decimal(8,4) ,
    `calculate_param` string,
    `calculate_type` string,
    `task_name` string,
    `line_id` int
    )
DUPLICATE KEY(`time`)
PARTITION BY RANGE(`time`) ()
DISTRIBUTED BY HASH(`time`) BUCKETS 1
PROPERTIES
(
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "dws_",
    "dynamic_partition.buckets" = "10",
    "replication_num" = "1"
);

CREATE TABLE IF NOT EXISTS test_db.dws_tag_second
(
    `time` datetime,
    `byt_name` string,
    `topic` string,
    `value` decimal(8,4) ,
    `calculate_param` string,
    `calculate_type` string,
    `task_name` string,
    `line_id` int
    )
    DUPLICATE KEY(`time`)
    PARTITION BY RANGE(`time`) ()
    DISTRIBUTED BY HASH(`time`) BUCKETS 1
    PROPERTIES
(
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "dws_",
    "dynamic_partition.buckets" = "10",
    "replication_num" = "1"
);