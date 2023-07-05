package com.byt.func;

import com.alibaba.fastjson.JSONObject;
import com.byt.pojo.TagKafkaInfo;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @title: post广播函数 value -> (postName,value)
 * @author: zhangyifan
 * @date: 2022/9/13 16:20
 */
public class Value2PNameAndValue
        extends BroadcastProcessFunction<TagKafkaInfo, JSONObject, Tuple2<String, TagKafkaInfo>> implements CDCState<HashSet<String>> {


    @Override
    public void processElement(TagKafkaInfo tagKafkaInfo, BroadcastProcessFunction<TagKafkaInfo, JSONObject, Tuple2<String, TagKafkaInfo>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<String, TagKafkaInfo>> collector) throws Exception {
        ReadOnlyBroadcastState<String, HashSet<String>> broadcastState = readOnlyContext.getBroadcastState(Descriptors.postDimDescriptor);
        HashSet<String> postNames = broadcastState.get(tagKafkaInfo.getBytName());
        if (postNames != null) {
            for (String postName : postNames) {
                collector.collect(Tuple2.of(postName, tagKafkaInfo));
            }
        }
    }

    @Override
    public void processBroadcastElement(JSONObject jsonObject, BroadcastProcessFunction<TagKafkaInfo, JSONObject, Tuple2<String, TagKafkaInfo>>.Context context, Collector<Tuple2<String, TagKafkaInfo>> collector) throws Exception {
        BroadcastState<String, HashSet<String>> broadcastState = context.getBroadcastState(Descriptors.postDimDescriptor);
        opHandler(jsonObject, broadcastState);
        // bytName -> postNames
    }

    /**
     *
     *
     *
     *
     */

    @Override
    public void broadcastStateAdd(JSONObject json, BroadcastState<String, HashSet<String>> broadcastState) throws Exception {
        String postName = json.getString("post_name");
        String[] postKeys = json.getString("post_keys").replaceAll("\\s*|\\t|\\r|\\n", "").split(","); // bytNames
        for (String key : postKeys) {
            if (!broadcastState.contains(key)) {
                broadcastState.put(key, Stream.of(postName).collect(Collectors.toCollection(HashSet::new)));
            } else {
                broadcastState.get(key).add(postName);
            }
        }
    }

    @Override
    public void broadcastStateDel(JSONObject jo, BroadcastState<String, HashSet<String>> broadcastState) throws Exception {
        String postName = jo.getString("post_name");
        String[] keys = jo.getString("post_keys").split(",");
        for (String k : keys) {
            if (broadcastState.contains(k)) {
                broadcastState.get(k).remove(postName);
            }
        }
    }
}