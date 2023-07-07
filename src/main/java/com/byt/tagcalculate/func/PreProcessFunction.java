package com.byt.tagcalculate.func;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.tagcalculate.pojo.TopicData;
import com.byt.common.protos.TagKafkaProtos;
import com.byt.common.utils.TimeUtil;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @title: 中间结果写回ods
 * @author: zhangyifan
 * @date: 2022/8/4 13:29
 */
public class PreProcessFunction extends ProcessWindowFunction<TagKafkaInfo, TopicData,String, TimeWindow> {
    @Override
    public void process(String s, ProcessWindowFunction<TagKafkaInfo, TopicData, String, TimeWindow>.Context context, Iterable<TagKafkaInfo> elements, Collector<TopicData> out) throws Exception {
        ArrayList<Value> lvOutBuild = new ArrayList<>();
        Iterator<TagKafkaInfo> iterator = elements.iterator();
        while (iterator.hasNext()) {
            TagKafkaInfo tag = iterator.next();
            TagKafkaProtos.TagKafkaInfo newTag = TagKafkaProtos.TagKafkaInfo.newBuilder()
                    .setName(tag.getBytName())
                    .setValue(tag.getValue().toString())
                    .setTime(TimeUtil.reformat(tag.getTime()))
                    .build();
            lvOutBuild.add(Value.newBuilder().mergeFrom(newTag.toByteArray()).build());

        }
        ListValue lvOut = ListValue.newBuilder()
                .addAllValues(lvOutBuild)
                .build();

        TopicData topicData = new TopicData();
        // TODO middle tag topic
        topicData.setTopic("tags_pre_test");
        topicData.setData(lvOut.toByteArray());
        out.collect(topicData);
    }
}
