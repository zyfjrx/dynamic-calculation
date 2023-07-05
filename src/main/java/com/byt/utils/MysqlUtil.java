package com.byt.utils;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @title:
 * @author: zhang
 * @date: 2022/7/15 15:35
 */
public class MysqlUtil {
    public static String getBaseDicLookUpDDL() {
        return "create table `tag`( " +
                "    `task_name` string," +
                "    `calculate_type` string, " +
                "    `time_gap` string " +
                ")" + MysqlUtil.mysqlLookUpTableDDL("source_tag");
    }

    public static String mysqlLookUpTableDDL(String tableName) {
        return "WITH ( " +
                "'connector' = 'jdbc', " +
                "'url' = 'jdbc:mysql://192.168.8.236:3306/byt_grid_data', " +
                "'table-name' = '" + tableName + "', " +
                "'lookup.cache.max-rows' = '100', " +
                "'lookup.cache.ttl' = '1 hour', " +
                "'username' = 'root', " +
                "'password' = 'root', " +
                "'driver' = 'com.mysql.cj.jdbc.Driver' " +
                ")";
    }

    public static String mysqlTableDDL(String tableName) {
        return "WITH ( " +
                "'connector' = 'jdbc', " +
                "'url' = 'jdbc:mysql://hadoop102:3306/flink_test', " +
                "'table-name' = '" + tableName + "', " +
                "'username' = 'root', " +
                "'password' = '000000', " +
                "'driver' = 'com.mysql.cj.jdbc.Driver' " +
                ")";
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境中设置为Kafka主题的分区数
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql(MysqlUtil.getBaseDicLookUpDDL());

        Table table = tableEnv.sqlQuery("select * from tag " +
                " where `task_name` = 'job1'");

        table.execute().print();

        env.execute();
    }
}
