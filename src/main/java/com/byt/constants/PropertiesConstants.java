package com.byt.constants;

/**
 * @title: 配置常量
 * @author: zhang
 * @date: 2022/8/16 15:00
 */
public class PropertiesConstants {
    public static final String ZHANGYIFAN = "zhangyifan";

    public static final String KAFKA_SERVER = "kafka.server";
    public static final String KAFKA_ODS_TOPIC = "kafka.ods.topic";
    public static final String KAFKA_DWD_TOPIC_PREFIX = "kafka.dwd.topic.prefix";
    public static final String KAFKA_DWS_TOPIC = "kafka.dws.topic";
    public static final String KAFKA_DWD_TOPIC = "kafka.dwd.topic";
    public static final String KAFKA_GROUP_ID = "kafka.group.id";
    public static final String DEFAULT_KAFKA_GROUP_ID = "randhn";


    public static final String DWS_TODAY_TABLE = "dws.today.table";
    public static final String DWS_RESULT_TABLE = "dws.result.table";
    public static final String DWS_SECOND_TABLE = "dws.second.table";
    //public static final String GREENPLUM_TODAY_TABLE_ASYNC = "greenplum.today.table.async";
    //public static final String GREENPLUM_RESULT_TABLE_ASYNC = "greenplum.result.table.async";
    //public static final String GREENPLUM_SECOND_TABLE_ASYNC = "greenplum.second.table.async";


    public static final String STREAM_PARALLELISM = "stream.parallelism";
    public static final String STREAM_CALCULATE_PARALLELISM = "stream.calculate.parallelism";
    public static final String STREAM_SINK_PG_PARALLELISM = "stream.sink.pg.parallelism";
    public static final String STREAM_SINK_DWS_PARALLELISM = "stream.sink.dws.parallelism";
    public static final String STREAM_SINK_DWD_PARALLELISM = "stream.sink.dwd.parallelism";
    public static final String STREAM_SOURCE_ODS_KAFKA_PARALLELISM = "stream.source.ods.kafka.parallelism";
    public static final String STREAM_SOURCE_DWD_KAFKA_PARALLELISM = "stream.source.dwd.kafka.parallelism";
    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
    public static final String STREAM_CHECKPOINTING_MODE = "stream.checkpointing.mode";
    public static final String STREAM_CHECKPOINT_DIR = "stream.checkpoint.dir";
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";
    public static final String STREAM_CHECKPOINT_MIN_PAUSE_BETWEEN_CHECKPOINTS = "stream.checkpoint.min.pause.between.checkpoints";
    public static final String STREAM_CHECKPOINT_TIMEOUT = "stream.checkpoint.timeout";
    public static final String STREAM_TIME_WINDOW = "stream.time.window";


    public static final String COMMON_PROPERTIES = "/common.properties";
    public static final String FLINK_DEV_PROPERTIES = "/application.properties";
    public static final String RUNTIME_ENVIRONMENT = "runtime_environment";

    // mysql
    public static final String MYSQL_HOST = "mysql.host";
    public static final String MYSQL_PORT = "mysql.port";
    public static final String MYSQL_USERNAME = "mysql.username";
    public static final String MYSQL_PASSWORD = "mysql.password";
    public static final String MYSQL_DATABASE = "mysql.database";
    public static final String MYSQL_TABLE = "mysql.table";
    public static final String MYSQL_POST_TABLE = "mysql.post.table";


    public static final String SENT_TIME = "sent.time";
    public static final String AVG = "AVG";
    public static final String MAX = "MAX";
    public static final String MIN = "MIN";
    public static final String LAST = "LAST";
    public static final String MEDIAN = "MEDIAN";
    public static final String CV = "CV";
    public static final String DEJUMP = "DEJUMP";
    public static final String FOF = "FOF";
    public static final String INTERP = "INTERP";
    public static final String TREND = "TREND";
    public static final String VAR = "VAR";
    public static final String PSEQ = "PSEQ";
    public static final String RANGE = "RANGE";
    public static final String SLOPE = "SLOPE";
    public static final String STD = "STD";











}
