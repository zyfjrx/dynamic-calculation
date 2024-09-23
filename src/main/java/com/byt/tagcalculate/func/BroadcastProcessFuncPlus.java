package com.byt.tagcalculate.func;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.byt.common.utils.BytTagUtil;
import com.byt.common.utils.JdbcUtil;
import com.byt.common.utils.QlexpressUtil;
import com.byt.tagcalculate.connection.impl.DataSourceGetter;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.tagcalculate.pojo.TagProperties;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.util.*;

/**
 * @title: 广播函数补充计算信息
 * @author: zhangyifan
 * @date: 2022/9/16 13:52
 */
public class BroadcastProcessFuncPlus extends BroadcastProcessFunction<List<TagKafkaInfo>, String, List<TagKafkaInfo>> {
    private MapStateDescriptor<String, TagProperties> mapStateDescriptor;
    private Map<String, TagProperties> bytInfoCache;
    private Set<String> hasTags;
    private Set<String> keys;
    private ParameterTool parameterTool;

    public BroadcastProcessFuncPlus(MapStateDescriptor<String, TagProperties> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        bytInfoCache = new HashMap<>(1024);
        hasTags = new HashSet<>();
        keys = new HashSet<>();
        parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        // 预加载配置信息，从mysql查询一次
        DruidPooledConnection connection = DataSourceGetter.getMysqlDataSource(
                parameterTool.get("mysql.host"),
                parameterTool.getInt("mysql.port"),
                parameterTool.get("mysql.username"),
                parameterTool.get("mysql.password"),
                parameterTool.get("mysql.database")
        ).getConnection();
        List<TagProperties> tagProperties = JdbcUtil.queryList(connection,
                "select * from `byt_grid_data`.tag_conf",
                TagProperties.class,
                false);
        for (TagProperties tagProperty : tagProperties) {
            String key = tagProperty.byt_name + tagProperty.task_name;
            bytInfoCache.put(key,tagProperty);
            keys.add(key);
            if (tagProperty.tag_name.contains(parameterTool.get("formula.tag.start")) &&
                    tagProperty.tag_name.contains(parameterTool.get("formula.tag.end"))) {
                Set<String> tagSet = QlexpressUtil.getTagSet(tagProperty.tag_name.trim());
                hasTags.addAll(tagSet);
            } else {
                hasTags.add(tagProperty.tag_name.trim());
            }
            Set<String> strings = bytInfoCache.keySet();
            for (String string : strings) {
                System.out.println(string+"---"+bytInfoCache.get(string));
            }

            connection.close();
        }
    }

    @Override
    public void processElement(List<TagKafkaInfo> value, BroadcastProcessFunction<List<TagKafkaInfo>, String, List<TagKafkaInfo>>.ReadOnlyContext ctx, Collector<List<TagKafkaInfo>> out) throws Exception {
        ReadOnlyBroadcastState<String, TagProperties> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 获取广播配置信息 封装为map
        for (String key : keys) {
            TagProperties tagProperties = broadcastState.get(key);
            TagProperties tagPropertiesMap = bytInfoCache.get(key);
            if (tagProperties != null || tagPropertiesMap != null){
                if (tagProperties == null) {
                    tagProperties = tagPropertiesMap;
                }
                bytInfoCache.put(key, tagProperties);
            }
        }
        // 调用工具类，处理补充信息
        List<TagKafkaInfo> bytTagData = BytTagUtil.bytTagData(value, hasTags, bytInfoCache, parameterTool);
        // 主流输出数据
        out.collect(bytTagData);
    }

    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<List<TagKafkaInfo>, String, List<TagKafkaInfo>>.Context ctx, Collector<List<TagKafkaInfo>> out) throws Exception {
        // 获取并解析数据，方便主流操作
        JSONObject jsonObject = JSON.parseObject(value);
        BroadcastState<String, TagProperties> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String op = jsonObject.getString("op");
        System.out.println(op);
        //todo 根据上线状态动态过滤已上线的配置，解决删除配置导致程序挂掉的问题
        if ("d".equals(op)) {
            TagProperties tagProperties = JSON.toJavaObject(jsonObject.getJSONObject("before"), TagProperties.class);
            String key = tagProperties.byt_name + tagProperties.task_name;
            broadcastState.remove(key);
            keys.remove(key);
        } else {
            TagProperties after = JSON.toJavaObject(jsonObject.getJSONObject("after"), TagProperties.class);
            TagProperties before = JSON.toJavaObject(jsonObject.getJSONObject("before"), TagProperties.class);
            if (before != null) {
                String brforeKey = before.byt_name + before.task_name;
                keys.remove(brforeKey);
                bytInfoCache.remove(brforeKey);
                broadcastState.remove(brforeKey);
            }
            String key = after.byt_name + after.task_name;
            if (after.tag_name.contains(parameterTool.get("formula.tag.start")) &&
                    after.tag_name.contains(parameterTool.get("formula.tag.end"))) {
                Set<String> tagSet = QlexpressUtil.getTagSet(after.tag_name.trim());
                hasTags.addAll(tagSet);
            } else {
                hasTags.add(after.tag_name.trim());
            }
            keys.add(key);
            bytInfoCache.put(key,after);
            broadcastState.put(key, after);
        }
        System.out.println("hasTags---->" + hasTags);
        System.out.println("keys---->" + keys);
    }

}
