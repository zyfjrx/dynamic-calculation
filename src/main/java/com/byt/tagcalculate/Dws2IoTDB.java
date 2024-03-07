package com.byt.tagcalculate;

import com.byt.common.utils.EnvironmentUtils;
import com.byt.common.utils.MyKafkaUtil;
import com.byt.tagcalculate.func.TagKafkaInfoToEventFunc;
import com.byt.tagcalculate.pojo.Event;
import com.byt.tagcalculate.sink.IotDBSink;
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


public class Dws2IoTDB {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境中,主题并行度与Kafka主题的分区数保持一致
        ParameterTool parameterTool = EnvironmentUtils.createParameterTool();
        env.getConfig().setGlobalJobParameters(parameterTool);
        SingleOutputStreamOperator<List<Event>> mainDS = env
                .addSource(MyKafkaUtil.getKafkaPojoConsumer(
                        parameterTool.get("kafka.dws.topic"),
                        "20240000000",
                        parameterTool.get("kafka.server")))
                .process(new TagKafkaInfoToEventFunc())
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(2L)))
                .process(new ProcessAllWindowFunction<Event, List<Event>, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Event, List<Event>, TimeWindow>.Context context, Iterable<Event> iterable, Collector<List<Event>> collector) throws Exception {
                        ArrayList<Event> list = Lists.newArrayList(iterable.iterator());
                        collector.collect(list);
                    }
                });
        //mainDS.print();
        mainDS.addSink(new IotDBSink()).setParallelism(2);
        env.execute(Dws2IoTDB.class.getSimpleName());

    }
}
