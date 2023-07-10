package com.byt.tagcalculate.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.byt.common.utils.BytTagUtil;
import com.byt.common.utils.FormulaTag;
import com.byt.common.utils.QlexpressUtil;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.tagcalculate.pojo.TagProperties;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * @title: 广播函数补充计算信息,动态创建输出表
 * @author: zhangyifan
 * @date: 2022/9/16 13:52
 */
public class BroadcastProcessBynamicTableFunc extends BroadcastProcessFunction<List<TagKafkaInfo>, String, List<TagKafkaInfo>> {
    private MapStateDescriptor<String, TagProperties> mapStateDescriptor;
    private Map<String, TagProperties> bytInfoCache;
    private Set<String> hasTags;
    private Set<String> keys;


    public BroadcastProcessBynamicTableFunc(MapStateDescriptor<String, TagProperties> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        bytInfoCache = new HashMap<>(1024);
        hasTags = new HashSet<>();
        keys = new HashSet<>();
    }

    @Override
    public void processElement(List<TagKafkaInfo> value, BroadcastProcessFunction<List<TagKafkaInfo>, String, List<TagKafkaInfo>>.ReadOnlyContext ctx, Collector<List<TagKafkaInfo>> out) throws Exception {
        ReadOnlyBroadcastState<String, TagProperties> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        // 获取广播配置信息 封装为map
        for (String key : keys) {
            TagProperties tagProperties = broadcastState.get(key);
            bytInfoCache.put(key, tagProperties);
        }

        // 调用工具类，处理补充信息
        List<TagKafkaInfo> bytTagData = BytTagUtil.bytTagData(value, hasTags, bytInfoCache);
        // 主流输出数据
        out.collect(bytTagData);
    }

    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<List<TagKafkaInfo>, String, List<TagKafkaInfo>>.Context ctx, Collector<List<TagKafkaInfo>> out) throws Exception {
        System.out.println(value + "--------------------------");
        // 获取并解析数据，方便主流操作
        JSONObject jsonObject = JSON.parseObject(value);
        TagProperties after = JSON.parseObject(jsonObject.getString("after"), TagProperties.class);
        BroadcastState<String, TagProperties> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String op = jsonObject.getString("op");
        String key = after.byt_name + after.task_name;
        //todo 根据上线状态动态过滤已上线的配置，解决删除配置导致程序挂掉的问题
        if (!op.equals("d") && after.status == 1) {
            keys.add(key);
            if (after.tag_name.contains(FormulaTag.START)) {
                Set<String> tagSet = QlexpressUtil.getTagSet(after.tag_name.trim());
                hasTags.addAll(tagSet);
            } else {
                hasTags.add(after.tag_name.trim());
            }
            broadcastState.put(key, after);
        } else if (op.equals("u")){
            broadcastState.put(key,after);
        }
        System.out.println("hasTags---->" + hasTags);
        System.out.println("keys---->" + keys);
    }


    public static void main(String[] args) {
        // {"id":19,"line_id":1,"username":"zhang","tag_name":"aj-aa","byt_name":"byt-a2","tag_topic":"opc-data","tag_type":"1","tag_desc":"测试","value_min":"0","value_max":"0","calculate_type":"DEJUMP_LAST","param":"-12,1000|2","post_send":0,"virtual_tag":0,"task_name":"tags_offline","create_time":1678957646000,"status":1}
        TagProperties after = JSON.parseObject("{\"id\":19,\"line_id\":1,\"username\":\"zhang\",\"tag_name\":\"aj-aa\",\"byt_name\":\"byt-a2\",\"tag_topic\":\"opc-data\",\"tag_type\":\"1\",\"tag_desc\":\"测试\",\"value_min\":\"0\",\"value_max\":\"0\",\"calculate_type\":\"DEJUMP_LAST\",\"param\":\"-12,1000|2\",\"post_send\":0,\"virtual_tag\":0,\"task_name\":\"tags_offline\",\"create_time\":1678957646000,\"status\":1}\n", TagProperties.class);
        System.out.println(after);
    }
}
