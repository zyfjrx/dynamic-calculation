package com.byt.tagwarning.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.byt.common.utils.TimeUtil;
import com.byt.tagwarning.pojo.TagProperties;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;
import java.util.Set;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/7/20 14:17
 **/
public class WarningBroadcastProcessFunc extends BroadcastProcessFunction<Map<String,Set<String>>,String,String> {
    private MapStateDescriptor<String, TagProperties> mapStateDescriptor;
    private OutputTag<Tuple2<String,String>> warningMsgTag;


    public WarningBroadcastProcessFunc(MapStateDescriptor<String, TagProperties> mapStateDescriptor, OutputTag<Tuple2<String,String>> warningMsgTag) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.warningMsgTag = warningMsgTag;
    }




    @Override
    public void processElement(Map<String, Set<String>> value, BroadcastProcessFunction<Map<String, Set<String>>, String, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
        ReadOnlyBroadcastState<String, TagProperties> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        for (String key : value.keySet()) {
            //List<String> confNames = broadcastState.get(key);
            TagProperties tagProperties = broadcastState.get(key);
            //System.out.println("conf:"+confNames);
            Set<String> srcNames = value.get(key);
            if (tagProperties != null){
                String[] confNames = tagProperties.tag_name.split(",");
                for (String confName : confNames) {
                    boolean flag = srcNames.contains(confName);
                    if (!flag){
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("tag",confName);
                        jsonObject.put("topic",tagProperties.tag_topic);
                        jsonObject.put("send_period",tagProperties.send_period);
                        jsonObject.put("send_times",tagProperties.send_times);
                        jsonObject.put("msg",confName+"最近一个小时值为空！");
                        jsonObject.put("ts",System.currentTimeMillis());
                        jsonObject.put("time",TimeUtil.getCurrentTimeString());
                        String msgKey = confName+"-"+ TimeUtil.getCurrentTimeStr();
                        jsonObject.put("key",msgKey);
                        ctx.output(warningMsgTag,Tuple2.of(msgKey,jsonObject.toJSONString()));
                        //System.out.println(msgKey+"----"+jsonObject.toJSONString());
                    }
                }
            }
//            Set<String> srcNames = value.get(key);
//            //System.out.println("src:"+srcNames);
//            if (confNames!= null){
//                for (String confName : confNames) {
//                    boolean flag = srcNames.contains(confName);
//                    if (!flag){
//                        JSONObject jsonObject = new JSONObject();
//                        jsonObject.put("tag",confName);
//                        jsonObject.put("topic",tagProperties.tag_topic);
//                        jsonObject.put("send_period",tagProperties.send_period);
//                        jsonObject.put("send_times",tagProperties.send_times);
//                        jsonObject.put("msg",confName+"最近一个小时值为空！");
//                        jsonObject.put("ts",System.currentTimeMillis());
//                        String msgKey = confName+"-"+ TimeUtil.getCurrentTimeStr();
//                        jsonObject.put("key",msgKey);
//                        ctx.output(warningMsgTag,Tuple2.of(msgKey,jsonObject.toJSONString()));
//                        //System.out.println(msgKey+"----"+jsonObject.toJSONString());
//                    }
//                }
//            }
        }

    }

    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<Map<String, Set<String>>, String, String>.Context ctx, Collector<String> out) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(value);
        String confData = jsonObject.getString("after");
        TagProperties tagProperties = null;
        if (confData != null){
            tagProperties = JSON.parseObject(confData, TagProperties.class);
        }
        BroadcastState<String, TagProperties> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        broadcastState.put(tagProperties.tag_topic, tagProperties);
    }


}
