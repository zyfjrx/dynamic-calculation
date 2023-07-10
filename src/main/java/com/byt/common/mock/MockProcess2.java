package com.byt.common.mock;

import com.byt.tagcalculate.pojo.TopicData;
import com.byt.common.protos.TagKafkaProtos;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Random;

/**
 * @title: 模拟数据发送
 * @author: zhangyifan
 * @date: 2022/10/14 08:59
 */
public class MockProcess2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<TopicData> result = env
                .addSource(new RichSourceFunction<MockBean2>() {
                    // 控制发送的个数和cancel程序
                    Integer times = 500000000;
                    private Random random = new Random();
                    // 待发送标签
                    private String[] tagNames = {"aj-8"};
                    private String[] values = {"True","False"};
                    @Override
                    public void run(SourceContext<MockBean2> ctx) throws Exception {
                        int i = 0;
                        while (times > 0) {
                            MockBean2 mockBean = new MockBean2(
                                    tagNames[random.nextInt(tagNames.length)],
                                    values[random.nextInt(values.length)],
                                    //random.nextInt(1000),
                                    //999,
                                    Calendar.getInstance().getTimeInMillis()
                            );
                            ctx.collect(mockBean);
                            System.out.println("发送数据：" + mockBean + ",发送时间：" + new Timestamp(mockBean.getTs()));
                            times--;
                            i++;
                            Thread.sleep(1000L);
                        }
                    }

                    @Override
                    public void cancel() {
                        times = -1;
                    }
                })
                .map(new RichMapFunction<MockBean2, Tuple3<String, String, String>>() {
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyyMMddHHmmss");
                    }

                    @Override
                    public Tuple3<String, String, String> map(MockBean2 value) throws Exception {
                        return Tuple3.of(value.getTagName(), value.getValue().toString(), sdf.format(value.getTs()));
                    }
                })
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(1L)))
                .process(new ProcessAllWindowFunction<Tuple3<String, String, String>, TopicData, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<Tuple3<String, String, String>, TopicData, TimeWindow>.Context context, Iterable<Tuple3<String, String, String>> elements, Collector<TopicData> out) throws Exception {
                        ArrayList<Value> lvOutBuild = new ArrayList<>();
                        Iterator<Tuple3<String, String, String>> iterator = elements.iterator();
                        while (iterator.hasNext()) {
                            Tuple3<String, String, String> tag = iterator.next();
                            TagKafkaProtos.TagKafkaInfo newTag = TagKafkaProtos.TagKafkaInfo.newBuilder()
                                    .setName(tag.f0)
                                    .setValue(tag.f1)
                                    .setTime(tag.f2)
                                    .build();
                            lvOutBuild.add(Value.newBuilder().mergeFrom(newTag.toByteArray()).build());

                        }
                        ListValue lvOut = ListValue.newBuilder()
                                .addAllValues(lvOutBuild)
                                .build();

                        TopicData topicData = new TopicData();
                        // 模拟数据发送的topic
                        topicData.setTopic("opc-data-ai");
                        topicData.setData(lvOut.toByteArray());

                        out.collect(topicData);
                    }
                });
        result
                .addSink(MyKafkaUtilMock.getProducerWithTopicData());


        env.execute();
    }
}
