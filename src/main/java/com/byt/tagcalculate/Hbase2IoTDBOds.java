package com.byt.tagcalculate;

import com.byt.common.utils.EnvironmentUtils;
import com.byt.tagcalculate.func.OdsTagToEventFunc;
import com.byt.tagcalculate.func.ProcessAllWindowFunc;
import com.byt.tagcalculate.pojo.Event;
import com.byt.tagcalculate.sink.IotDBSink;
import com.byt.tagcalculate.source.HbaseSource;
import com.google.common.collect.Lists;
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
public class Hbase2IoTDBOds {
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
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1L)))
                .process(new ProcessAllWindowFunc());
        //mainDS.print();

        mainDS.addSink(new IotDBSink()).setParallelism(2);
        env.execute(Hbase2IoTDBOds.class.getSimpleName());

    }
}
