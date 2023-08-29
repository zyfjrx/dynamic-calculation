package com.byt.tagcalculate.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.byt.common.utils.BytTagUtil;
import com.byt.common.utils.FormulaTag;
import com.byt.common.utils.QlexpressUtil;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.tagcalculate.pojo.TagProperties;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * @title: 广播函数补充计算信息
 * @author: zhangyifan
 * @date: 2022/9/16 13:52
 */
public class BroadcastProcessFunc extends BroadcastProcessFunction<List<TagKafkaInfo>, String, List<TagKafkaInfo>> implements PropertiesCdc<TagProperties>{
    private MapStateDescriptor<String, TagProperties> mapStateDescriptor;
    //private String startjobs;
    private Map<String, TagProperties> bytInfoCache;
    private Set<String> hasTags;
    private Set<String> keys;
    private ParameterTool parameterTool;


    public BroadcastProcessFunc(MapStateDescriptor<String, TagProperties> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        bytInfoCache = new HashMap<>(1024);
        hasTags = new HashSet<>();
        keys = new HashSet<>();
        parameterTool = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
    }

    @Override
    public void processElement(List<TagKafkaInfo> value, BroadcastProcessFunction<List<TagKafkaInfo>, String, List<TagKafkaInfo>>.ReadOnlyContext ctx, Collector<List<TagKafkaInfo>> out) throws Exception {
        ReadOnlyBroadcastState<String, TagProperties> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 获取广播配置信息 封装为map
        for (String key : keys) {
            TagProperties tagProperties = broadcastState.get(key);
            bytInfoCache.put(key, tagProperties);
        }
        // 主流数据先到等待配置3s
        if (bytInfoCache.keySet().size() == 0) {
            System.out.println("wait~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
            Thread.sleep(3000L);
        }

        // 调用工具类，处理补充信息
        List<TagKafkaInfo> bytTagData = BytTagUtil.bytTagData(value, hasTags, bytInfoCache, parameterTool);
        // 主流输出数据
        out.collect(bytTagData);
    }

    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<List<TagKafkaInfo>, String, List<TagKafkaInfo>>.Context ctx, Collector<List<TagKafkaInfo>> out) throws Exception {
        BroadcastState<String, TagProperties> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        JSONObject jsonObject = JSON.parseObject(value);
        opHandler(jsonObject,broadcastState);
        // 获取并解析数据，方便主流操作
       /* JSONObject jsonObject = JSON.parseObject(value);
        System.out.println("json>>>>>:" + jsonObject);
        String op = jsonObject.getString("op");
        TagProperties after = JSON.parseObject(jsonObject.getString("after"), TagProperties.class);
        if (after != null) {
            after.setOp(op);
            String key = after.byt_name + after.task_name;
            keys.add(key);
            if (after.tag_name.startsWith(parameterTool.get("formula.tag.start"))) {
                Set<String> tagSet = QlexpressUtil.getTagSet(after.tag_name.trim());
                hasTags.addAll(tagSet);
            } else {
                hasTags.add(after.tag_name.trim());
            }
            broadcastState.put(key, after);
        } else {
            TagProperties before = JSON.parseObject(jsonObject.getString("before"), TagProperties.class);
            String key = before.byt_name + before.task_name;
            broadcastState.remove(key);
        }
        System.out.println("after:" + after);
        System.out.println("hasTags---->" + hasTags);
        System.out.println("keys---->" + keys);*/
        System.out.println("hasTags---->" + hasTags);
        System.out.println("keys---->" + keys);
    }

    @Override
    public void broadcastStateAdd(JSONObject json, BroadcastState<String, TagProperties> broadcastState) throws Exception {
        TagProperties tagProperties = json.toJavaObject(TagProperties.class);
        String key = tagProperties.byt_name + tagProperties.task_name;
        keys.add(key);
        if (tagProperties.tag_name.startsWith(parameterTool.get("formula.tag.start"))) {
            Set<String> tagSet = QlexpressUtil.getTagSet(tagProperties.tag_name.trim());
            hasTags.addAll(tagSet);
        } else {
            hasTags.add(tagProperties.tag_name.trim());
        }
        broadcastState.put(key, tagProperties);
    }

    @Override
    public void broadcastStateDel(JSONObject json, BroadcastState<String, TagProperties> broadcastState) throws Exception {
        TagProperties tagProperties = json.toJavaObject(TagProperties.class);
        String key = tagProperties.byt_name + tagProperties.task_name;
        broadcastState.remove(key);
    }
}
