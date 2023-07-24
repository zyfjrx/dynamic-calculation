-- auto-generated definition
create table dws_tag_today
(
    byt_name         varchar(255) null,
    tag_topic        varchar(255) null,
    value            decimal(20,4) null,
    calculate_time   timestamp    null,
    calculate_type   varchar(255) null,
    calculate_params varchar(255) null,
    job_name         varchar(50)  null,
    line_id          int          null
);

create table dws_tag_result
(
    byt_name         varchar(255) null,
    tag_topic        varchar(255) null,
    value            decimal(20,4) null,
    calculate_time   timestamp    null,
    calculate_type   varchar(255) null,
    calculate_params varchar(255) null,
    job_name         varchar(50)  null,
    line_id          int          null
);

create table dws_tag_second
(
    byt_name         varchar(255) null,
    tag_topic        varchar(255) null,
    value            double       null,
    calculate_time   timestamp    null,
    calculate_type   varchar(255) null,
    calculate_params varchar(255) null,
    job_name         varchar(50)  null,
    line_id          int          null
);

-- 索引
CREATE INDEX index_create_time ON dws_tag_second (calculate_time)