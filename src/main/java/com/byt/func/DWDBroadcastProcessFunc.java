package com.byt.func;

import akka.actor.TimerSchedulerImpl;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.byt.pojo.TagKafkaInfo;
import com.byt.pojo.TagProperties;
import com.byt.utils.BytTagUtil;
import com.byt.utils.FormulaTag;
import com.byt.utils.QlexpressUtil;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * @title: 广播函数补充计算信息
 * @author: zhangyifan
 * @date: 2022/9/16 13:52
 */
public class DWDBroadcastProcessFunc extends BroadcastProcessFunction<List<TagKafkaInfo>, String, Tuple2<String, List<TagKafkaInfo>>> {
    private MapStateDescriptor<String, TagProperties> mapStateDescriptor;
    //private String startjobs;
    private Map<String, TagProperties> bytInfoCache;
    private Set<String> hasTags;
    private String jobName;
    private ListState<String> keyStates;




    public DWDBroadcastProcessFunc(MapStateDescriptor<String, TagProperties> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        bytInfoCache = new HashMap<>(1024);
        hasTags = new HashSet<>();
        keyStates = getRuntimeContext().getListState(
                new ListStateDescriptor<String>(
                        "key-state",
                        Types.STRING
                )
        );
    }

    @Override
    public void processElement(List<TagKafkaInfo> value, BroadcastProcessFunction<List<TagKafkaInfo>, String, Tuple2<String, List<TagKafkaInfo>>>.ReadOnlyContext ctx, Collector<Tuple2<String, List<TagKafkaInfo>>> out) throws Exception {
        ReadOnlyBroadcastState<String, TagProperties> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        Iterator<String> iterator = keyStates.get().iterator();
        while (iterator.hasNext()){
            String keyInfo = iterator.next();
            TagProperties tagProperties = broadcastState.get(keyInfo);
            // 保存所有配置信息
            bytInfoCache.put(keyInfo, tagProperties);
        }

        // 调用工具类，处理补充信息
        List<TagKafkaInfo> bytTagData = BytTagUtil.bytTagData(value, hasTags, bytInfoCache);
        //System.out.println("<><><><>"+bytTagData);
        String normalState = null;
        if (!bytTagData.isEmpty()) {
            normalState = bytTagData.get(0).getIsNormal().equals(1) ? "normal" : "abnormal";
        }

        //System.out.println(bytTagData);
        // 主流输出数据
        out.collect(Tuple2.of(normalState, bytTagData));
    }

    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<List<TagKafkaInfo>, String, Tuple2<String, List<TagKafkaInfo>>>.Context ctx, Collector<Tuple2<String, List<TagKafkaInfo>>> out) throws Exception {


        // 获取并解析数据，方便主流操作
        JSONObject jsonObject = JSON.parseObject(value);
        TagProperties after = JSON.parseObject(jsonObject.getString("after"), TagProperties.class);
        BroadcastState<String, TagProperties> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
        String op = jsonObject.getString("op");
        String afterKeyInfo = after.byt_name + after.task_name;
        //todo 根据上线状态动态过滤已上线的配置，解决删除配置导致程序挂掉的问题
        if (!op.equals("d") && after.status == 1) {
            //keyInfoList.add(afterKeyInfo.trim());
            keyStates.add(afterKeyInfo.trim());
            if (after.tag_name.contains(FormulaTag.START)) {
                Set<String> tagSet = QlexpressUtil.getTagSet(after.tag_name.trim());
                hasTags.addAll(tagSet);
            } else {
                hasTags.add(after.tag_name.trim());
            }
            broadcastState.put(afterKeyInfo, after);
        }
//        if (op.equals("d")) {
//            String beforeKeyInfo = before.byt_name + before.task_name;
//            keyInfoList.remove(beforeKeyInfo);
//
//            if (before.tag_name.contains(FormulaTag.START)) {
//                Set<String> tagSet = QlexpressUtil.getTagSet(before.tag_name);
//                hasTags.removeAll(tagSet);
//            } else {
//                hasTags.remove(before.tag_name);
//            }
//            broadcastState.remove(beforeKeyInfo);
//        } else if (after.status==0){
//            String afterKeyInfo = after.byt_name + after.task_name;
//            keyInfoList.remove(afterKeyInfo);
//
//            if (after.tag_name.contains(FormulaTag.START)) {
//                Set<String> tagSet = QlexpressUtil.getTagSet(after.tag_name);
//                hasTags.removeAll(tagSet);
//            } else {
//                hasTags.remove(after.tag_name);
//            }
//            broadcastState.remove(afterKeyInfo);
//        }
//        else if (op.equals("u")) {
//            String beforeKeyInfo = before.byt_name + before.task_name;
//            String afterKeyInfo = after.byt_name + after.task_name;
//            keyInfoList.remove(beforeKeyInfo);
//            keyInfoList.add(afterKeyInfo.trim());
//            if (after.tag_name.contains(FormulaTag.START)) {
//                Set<String> beforeTagSet = QlexpressUtil.getTagSet(before.tag_name.trim());
//                Set<String> afterTagSet = QlexpressUtil.getTagSet(after.tag_name.trim());
//                hasTags.removeAll(beforeTagSet);
//                hasTags.addAll(afterTagSet);
//            } else {
//                hasTags.remove(before.tag_name);
//                hasTags.add(after.tag_name.trim());
//            }
//            broadcastState.remove(beforeKeyInfo);
//            broadcastState.put(afterKeyInfo, after);
//        }
//        else {
//            String afterKeyInfo = after.byt_name + after.task_name;
//            keyInfoList.add(afterKeyInfo.trim());
//            if (after.tag_name.contains(FormulaTag.START)) {
//                Set<String> tagSet = QlexpressUtil.getTagSet(after.tag_name.trim());
//                hasTags.addAll(tagSet);
//            } else {
//                hasTags.add(after.tag_name.trim());
//            }
//            broadcastState.put(afterKeyInfo, after);
//        }

        // old
        /*if (tagProperties.status == 1) {
            //System.out.println("tagProperties" + tagProperties);
            if (tagProperties.tag_name.contains(FormulaTag.START)) {
                Set<String> tagSet = QlexpressUtil.getTagSet(tagProperties.tag_name);
                hasTags.addAll(tagSet);
            } else {
                hasTags.add(tagProperties.tag_name);
            }
            keyInfoList.add(keyInfo);
        } else if (tagProperties.status == 0) {
            if (tagProperties.tag_name.contains(FormulaTag.START)) {
                Set<String> tagSet = QlexpressUtil.getTagSet(tagProperties.tag_name);
                hasTags.removeAll(tagSet);
            } else {
                hasTags.remove(tagProperties.tag_name);
            }
            keyInfoList.remove(keyInfo);
        }

        System.out.println("tagProperties" + tagProperties);
        System.out.println("nameList>>>>>>" + hasTags);
        System.out.println("key>>>>>>>>>" + keyInfo);
         // 将配置表信息写入广播状态
        broadcastState.put(keyInfo, tagProperties);
*/

        System.out.println("nameList>>>>>>" + hasTags);
        System.out.println("keylist:>>>>" + keyStates);
    }
}
