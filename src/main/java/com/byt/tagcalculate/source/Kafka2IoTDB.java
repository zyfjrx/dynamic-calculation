package com.byt.tagcalculate.source;

import com.alibaba.fastjson.JSON;
import com.byt.common.utils.EnvironmentUtils;
import com.byt.common.utils.MyKafkaUtil;
import com.byt.tagcalculate.func.TagKafkaInfoToEventFunc3;
import com.byt.tagcalculate.pojo.Event;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.tagcalculate.sink.IotDBSink;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/20 09:35
 **/
public class Kafka2IoTDB {
    public static void main(String[] args) throws Exception {
        // 获取执行环境和相关参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //System.setProperty("HADOOP_USER_NAME", "root");
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        env.getConfig().setGlobalJobParameters(parameterTool);

        SingleOutputStreamOperator<List<Event>> mainDS = env
                .addSource(
                        MyKafkaUtil
                                .getKafkaListConsumer(Arrays.asList(parameterTool.get("kafka.ods.topic").split(",")),
                                        "test1_20230808", parameterTool.get("kafka.server")
                                ))
                .flatMap(new FlatMapFunction<List<TagKafkaInfo>, TagKafkaInfo>() {
                    @Override
                    public void flatMap(List<TagKafkaInfo> tagKafkaInfos, Collector<TagKafkaInfo> collector) throws Exception {
                        for (TagKafkaInfo tagKafkaInfo : tagKafkaInfos) {
                            collector.collect(tagKafkaInfo);
                        }
                    }
                })
                .process(new TagKafkaInfoToEventFunc3())
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1L)))
                .process(new ProcessAllWindowFunction<Event, List<Event>, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Event, List<Event>, TimeWindow>.Context context, Iterable<Event> iterable, Collector<List<Event>> collector) throws Exception {
                        ArrayList<Event> list = Lists.newArrayList(iterable.iterator());
                        System.out.println(JSON.toJSONString(list));
                        collector.collect(list);
                    }
                });

        mainDS.print();

        mainDS.addSink(new IotDBSink()).setParallelism(2);
        env.execute();

    }
}
