package com.byt.tagcalculate.source;

import com.alibaba.fastjson.JSON;
import com.byt.common.utils.EnvironmentUtils;
import com.byt.tagcalculate.func.OdsTagToEventFunc;
import com.byt.tagcalculate.pojo.Event;
import com.byt.tagcalculate.sink.IotDBSink;
import com.google.common.collect.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @title:
 * @author: zhangyf
 * @date: 2024/3/20 09:35
 **/
public class Hbase2IoTDB {
    public static void main(String[] args) throws Exception {
        // 获取执行环境和相关参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        System.setProperty("HADOOP_USER_NAME", "root");
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        env.getConfig().setGlobalJobParameters(parameterTool);

        SingleOutputStreamOperator<List<Event>> mainDS = env
                .addSource(new HbaseSource())
                .process(new OdsTagToEventFunc())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.getTimestamp();
                                    }
                                })
                )
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
