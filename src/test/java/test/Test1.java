package test;

import com.byt.tagcalculate.calculate.dynamicwindow.DynamicSlidingEventTimeWindows;
import com.byt.tagcalculate.calculate.dynamicwindow.TimeAdjustExtractor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @title:
 * @author: zhangyf
 * @date: 2023/7/19 14:15
 **/
public class Test1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, TestPojo>() {
                    @Override
                    public TestPojo map(String s) throws Exception {
                        String[] split = s.split(",");
                        return TestPojo.of(
                                split[0],
                                Integer.valueOf(split[1]),
                                Long.parseLong(split[2]),
                                Long.parseLong(split[2]),
                                Long.parseLong(split[2])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TestPojo>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<TestPojo>() {
                                    @Override
                                    public long extractTimestamp(TestPojo testPojo, long l) {
                                        return testPojo.timestamp;
                                    }
                                })

                )
                .keyBy(r -> r.word)
                .window(DynamicSlidingEventTimeWindows.of(
                        // Pass in the default time information, used when there is no data stream
                        Time.seconds(5L), Time.seconds(1L),
                        // Extract time information from data streams
                        new TimeAdjustExtractor<TestPojo>() {
                            @Override
                            public long extract(TestPojo element) {
                                return element.winSlide;
                            }
                        },
                        new TimeAdjustExtractor<TestPojo>() {
                            @Override
                            public long extract(TestPojo element) {
                                return element.winSlide;
                            }
                        }

                ))
                .sum("value")
                .print();
        env.execute();
    }
}
