package com.byt.tagcalculate.hudi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @title: hudi探索
 * @author: zhangyifan
 * @date: 2022/9/23 09:28
 */
public class Ods2DwdJOb {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // CDC 读取配置表
        tableEnv
                .executeSql("create table tag (" +
                        "   id int, " +
                        "   line_id int," +
                        "   tag_name string," +
                        "   byt_name string," +
                        "   tag_desc string," +
                        "   value_min string," +
                        "   value_max string," +
                        "   calculate_type string," +
                        "   param string," +
                        "   task_name string," +
                        "   create_time string," +
                        "   status int," +
                        "   primary key(id) not enforced " +
                        ") WITH (" +
                        "   'connector' = 'mysql-cdc'," +
                        "   'hostname' = '192.168.8.236'," +
                        "   'port' = '3306'," +
                        "   'username' = 'root'," +
                        "   'password' = 'root'," +
                        "   'database-name' = 'byt_grid_data'," +
                        "   'table-name' = 'tag_conf'" +
                        ")");





        tableEnv
                .executeSql("select * from tag").print();




       // env.execute();
    }
}





// 时间轴
// 文件管理
// 索引Index ：全局索引；非全局索引
// 表的类型：默认Copy On Write
        /*
        1.计算模型：批式模型 流式模型 增量模型
        2.查询方式：快照查询 增量查询 读优化查询
        3.
        COW(copy on write): parquet文件
        MOR(merge on read)：delta log 和 parquet
        写入数据方式：upsert ,insert, bulk insert
         */