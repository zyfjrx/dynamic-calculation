package com.byt.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.byt.cdc.FlinkCDC;
import com.byt.pojo.PostSetup;
import com.byt.pojo.TagKafkaInfo;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

/**
 * @title:
 * @author: zhangyifan
 * @date: 2022/8/14 08:53
 */
public class PostJsonFunc extends
        KeyedBroadcastProcessFunction<String, Tuple2<String, TagKafkaInfo>, JSONObject, Tuple2<String, String>>
        implements CDCState<PostSetup> {

    private transient MapState<String, HashMap<String, String>> jsonState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, HashMap<String, String>> mapStateDescriptor = new MapStateDescriptor<String, HashMap<String, String>>(
                "jsonState",
                BasicTypeInfo.STRING_TYPE_INFO,
                TypeInformation.of(new TypeHint<HashMap<String, String>>() {
                })
        );
        StateTtlConfig stateTtlConfig = new StateTtlConfig
                .Builder(Time.seconds(3))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .build();
        mapStateDescriptor.enableTimeToLive(stateTtlConfig);
        jsonState = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    public void addToJsonState(MapState<String, HashMap<String, String>> jsonState, String stateKey, String bytName, BigDecimal value) throws Exception {
        HashMap<String, String> map = jsonState.get(stateKey);
        if (map == null) {
            map = new HashMap<>();
            map.put(bytName, value.toString());
            jsonState.put(stateKey, map);
        } else {
            jsonState.get(stateKey).put(bytName, value.toString());
        }
    }


    @Override
    public void processElement(Tuple2<String, TagKafkaInfo> data, KeyedBroadcastProcessFunction<String, Tuple2<String, TagKafkaInfo>, JSONObject, Tuple2<String, String>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<String, String>> collector) throws Exception {
        String postName = data.f0;
        TagKafkaInfo tagKafkaInfo = data.f1;
        String stateKay = tagKafkaInfo.getTime();
        ReadOnlyBroadcastState<String, PostSetup> broadcastState = readOnlyContext.getBroadcastState(Descriptors.postMapDescriptor);
        PostSetup postSetup = broadcastState.get(postName);
        addToJsonState(jsonState, stateKay, tagKafkaInfo.getBytName(), tagKafkaInfo.getValue());
        if (jsonState.get(stateKay) != null && (jsonState.get(stateKay).size() == postSetup.getPostKeys().size())) {
            HashMap<String, String> map = jsonState.get(stateKay);
            map.put("create_time",tagKafkaInfo.getTime());
            collector.collect(Tuple2.of(postSetup.getPostUrl(), JSON.toJSONString(map)));
            jsonState.remove(stateKay);
        }
    }

    @Override
    public void processBroadcastElement(JSONObject jsonObject, KeyedBroadcastProcessFunction<String, Tuple2<String, TagKafkaInfo>, JSONObject, Tuple2<String, String>>.Context context, Collector<Tuple2<String, String>> collector) throws Exception {
        BroadcastState<String, PostSetup> broadcastState = context.getBroadcastState(Descriptors.postMapDescriptor);
        opHandler(jsonObject, broadcastState);
    }

    @Override
    public void broadcastStateAdd(JSONObject jo, BroadcastState<String, PostSetup> broadcastState) throws Exception {
        PostSetup postSetup = new PostSetup();
        postSetup.setPostName(jo.getString("post_name"));
        postSetup.setPostKeys(new HashSet<>(Arrays.asList(jo.getString("post_keys").split(","))));
        postSetup.setPostUrl(jo.getString("post_url"));
        broadcastState.put(jo.getString("post_name"), postSetup);
    }

    @Override
    public void broadcastStateDel(JSONObject jo, BroadcastState<String, PostSetup> broadcastState) throws Exception {
        broadcastState.remove(jo.getString("post_name"));
    }
}
