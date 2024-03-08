package com.byt.common.utils;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import java.util.Properties;

public class DorisUtil {

    public static SinkFunction<String> getDorisSink(String fenodes,String user,String pwd,String table) {

        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据
        props.setProperty("strip_outer_array", "true");

        return DorisSink.sink(
                new DorisExecutionOptions.Builder()
                        //.setBatchIntervalMs(5000L)
                        //.setBatchSize(500)
                        .setEnableDelete(false)
                        .setMaxRetries(3)
                        .setStreamLoadProp(props)
                        .build(),
                new DorisOptions.Builder()
                        .setFenodes(fenodes)
                        .setUsername(user)
                        .setPassword(pwd)
                        .setTableIdentifier(table)
                        .build()
        );
    }
}

