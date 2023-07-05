package com.byt.calculate;

import com.byt.pojo.TagKafkaInfo;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/7/4 13:56
 **/
public class DynamicTimeWindowFunction extends KeyedProcessFunction<Tuple7<String, String, String, String, String, Integer, String>, TagKafkaInfo,TagKafkaInfo> {

    // 获取


    @Override
    public void open(Configuration parameters) throws Exception {

    }

    @Override
    public void processElement(TagKafkaInfo tagKafkaInfo, KeyedProcessFunction<Tuple7<String, String, String, String, String, Integer, String>, TagKafkaInfo, TagKafkaInfo>.Context context, Collector<TagKafkaInfo> collector) throws Exception {

    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Tuple7<String, String, String, String, String, Integer, String>, TagKafkaInfo, TagKafkaInfo>.OnTimerContext ctx, Collector<TagKafkaInfo> out) throws Exception {

    }
}
