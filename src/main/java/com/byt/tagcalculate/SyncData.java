package com.byt.tagcalculate;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.byt.common.cdc.FlinkCDC;
import com.byt.common.utils.EnvironmentUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @title: 数据同步
 * @author: zhangyf
 * @date: 2024/6/4 9:46
 **/
public class SyncData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataStreamSource<String> mysqlCDCSource = env  // 读取配置流
                .fromSource(FlinkCDC.getMysqlSource(
                        parameterTool.get("sync.mysql.host"),
                        parameterTool.getInt("sync.mysql.port"),
                        parameterTool.get("sync.mysql.username"),
                        parameterTool.get("sync.mysql.password"),
                        parameterTool.get("sync.mysql.database"),
                        parameterTool.get("sync.mysql.table").split(",")
                ), WatermarkStrategy.noWatermarks(), "mysql");

        SingleOutputStreamOperator<String> result = mysqlCDCSource
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);
                        JSONObject after = jsonObject.getJSONObject("after");
                        String table = jsonObject.getJSONObject("source").getString("table");
                        after.put("table",table);
                        //String after = JSON.toJSONString(jsonObject.getJSONObject("after"));
                        return after.toJSONString();
                    }
                });

        result.print();


        env.execute(SyncData.class.getSimpleName());
    }
}
