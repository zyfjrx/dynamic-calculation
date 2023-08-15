package com.byt.common.cdc;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.byt.common.utils.ConfigManager;
import com.byt.tagcalculate.constants.PropertiesConstants;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Set;

/**
 * @title: 实时同步配置
 * @author: zhang
 * @date: 2022/8/16 13:37
 */
public class FlinkCDC {

        public static MySqlSource<String> getMysqlSource(String host,Integer port,String username,String password,String database,String table){
            return MySqlSource
                    .<String>builder()
                    .hostname(host)
                    .port(port)
                    .username(username)
                    .password(password)
                    .databaseList(database)
                    .tableList(table)
                    .deserializer(new JsonDebeziumDeserializationSchema())
                    .startupOptions(StartupOptions.initial())
                    .build();
        }



    public static MySqlSource<String> getMysqlSourceWithPost(){
        return MySqlSource
                .<String>builder()
                .hostname(ConfigManager.getProperty(PropertiesConstants.MYSQL_HOST))
                .port(ConfigManager.getInteger(PropertiesConstants.MYSQL_PORT))
                .username(ConfigManager.getProperty(PropertiesConstants.MYSQL_USERNAME))
                .password(ConfigManager.getProperty(PropertiesConstants.MYSQL_PASSWORD))
                .databaseList(ConfigManager.getProperty(PropertiesConstants.MYSQL_DATABASE))
                .tableList(ConfigManager.getProperty(PropertiesConstants.MYSQL_POST_TABLE))
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
    }

//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        env  // 读取配置流
//                .fromSource(FlinkCDC.getMysqlSource(), WatermarkStrategy.noWatermarks(),"mysql")
//                .map(new MapFunction<String, String>() {
//                    @Override
//                    public String map(String jsonStr) throws Exception {
//                        JSONObject jsonObj = JSON.parseObject(jsonStr);
//                        JSONObject before = jsonObj.getJSONObject("before");
//                        JSONObject after = jsonObj.getJSONObject("after");
//
//                        ArrayList<String> oldKeys = new ArrayList<>();
//                        JSONObject old = null;
//                        if (before != null && after != null) {
//                            old = new JSONObject();
//                            Set<String> allKeys = before.keySet();
//                            for (String key : allKeys) {
//                                if ((before.getString(key) == null &&
//                                        after.getString(key) != null) ||
//                                        (before.getString(key) != null &&
//                                                !before.getString(key).equals(after.getString(key)))) {
//                                    oldKeys.add(key);
//                                }
//                            }
//
//                            for (String oldKey : oldKeys) {
//                                old.put(oldKey, before.getString(oldKey));
//                            }
//                        }
//
//                        String op = jsonObj.getString("op");
//                        String type = null;
//                        switch (op) {
//                            case "u":
//                                type = "update";
//                                break;
//                            case "c":
//                            case "r":
//                                type = "insert";
//                                break;
//                            case "d":
//                                type = "delete";
//                                break;
//                        }
//
//                        JSONObject source = jsonObj.getJSONObject("source");
//                        String table = source.getString("table");
//
//                        //时间
//                        Long tsMs = jsonObj.getLong("ts_ms");
//
//                        JSONObject mxwJsonObj = new JSONObject();
//                        mxwJsonObj.put("table", table);
//                        mxwJsonObj.put("type", type);
//                        mxwJsonObj.put("ts", tsMs);
//                        mxwJsonObj.put("data", after);
//                        mxwJsonObj.put("old", old);
//
//                        return mxwJsonObj.toJSONString();
//                    }
//                })
//                .print();
//
//        env.execute();
//    }
}
