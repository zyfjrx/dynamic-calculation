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
    value            decimal(20,4) null,
    calculate_time   timestamp    null,
    calculate_type   varchar(255) null,
    calculate_params varchar(255) null,
    job_name         varchar(50)  null,
    line_id          int          null
);

-- 索引
CREATE INDEX index_create_time ON dws_tag_second (calculate_time)


create table tag_conf
(
    id             int auto_increment
        primary key,
    line_id        int                          null comment 'tag对应的产线id',
    username       varchar(255) charset utf8mb4 null comment '创建人',
    tag_name       varchar(255)                 null comment '原始标签',
    byt_name       varchar(255)                 null comment '计算后标签',
    tag_topic      varchar(255)                 null comment '原始标签来源kafka主题',
    tag_type       varchar(255) default '1'     null comment '1瞬时值2累计值',
    tag_desc       varchar(255)                 null comment '标签描述',
    value_min      varchar(255) default '0'     null comment '标签原始值的下限',
    value_max      varchar(255) default '0'     null comment '标签原始值上限',
    calculate_type varchar(255)                 null comment '算子类型',
    param          varchar(255)                 null comment '计算参数，每个算子间参数｜隔开，算子内部用逗号隔开',
    post_send      int(5)       default 0       null comment 'post发送，0-否，1-是',
    virtual_tag    int(5)       default 0       null comment '中间标签，0-否，1-是',
    task_name      varchar(255)                 null comment '对应job',
    create_time    datetime                     null comment '创建时间',
    status         int          default 0       null comment '0:下线；1：上线',
    constraint tag_conf_byt_name_uindex
        unique (byt_name)
) comment '数据预处理标签配置' ENGINE=InnoDB CHARSET=utf8mb4;