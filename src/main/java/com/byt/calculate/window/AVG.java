package com.byt.calculate.window;

import com.byt.calculate.*;
import com.byt.constants.PropertiesConstants;
import com.byt.pojo.TagKafkaInfo;
import com.byt.utils.ConfigManager;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * @title: 平均值算子
 * @author: zhangyifan
 * @date: 2022/8/17 09:02
 */
public class AVG implements Transform {
    private String size;
    private String slide;

    public AVG(List<String> params) {
        this.size = params.get(0);
        this.slide = params.get(1);
    }

    public AVG() {
    }

    public DataStream<TagKafkaInfo> avg(DataStream<TagKafkaInfo> in, String size, String slide) {
        return in
                //.keyBy(r -> Tuple6.of(r.getName(), r.getTopic(), r.getBytName(), r.getTimeGap(), r.getSlideGap(), r.getCalculateType()))
                .keyBy(new KeySelector<TagKafkaInfo, Tuple7<String, String, String, String, String, Integer, String>>() {
                    @Override
                    public Tuple7<String, String, String, String, String, Integer, String> getKey(TagKafkaInfo r) throws Exception {
                        return Tuple7.of(r.getName(), r.getTopic(), r.getBytName(), r.getCalculateParam(), r.getCalculateType(), r.getLineId(), r.getTaskName());
                    }
                })
                .window(DynamicSlidingEventTimeWindows.of(Time.seconds(10L),Time.seconds(1L)))
                .aggregate(
                        new AvgAgg(),
                        new AvgResult()
                ).setParallelism(ConfigManager.getInteger(PropertiesConstants.STREAM_CALCULATE_PARALLELISM))
                .name("AVG-平均值-window("+size+","+slide+")");
    }

    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = avg(tStream.stream, size, slide);
        chain.doTransform(tStream);
    }


    class AvgResult extends ProcessWindowFunction<BigDecimal, TagKafkaInfo, Tuple7<String, String, String, String, String, Integer, String>, TimeWindow> {
        private SimpleDateFormat sdf;

        @Override
        public void open(Configuration parameters) throws Exception {
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }


        @Override
        public void process(Tuple7<String, String, String, String, String, Integer, String> key, ProcessWindowFunction<BigDecimal, TagKafkaInfo, Tuple7<String, String, String, String, String, Integer, String>, TimeWindow>.Context context, Iterable<BigDecimal> elements, Collector<TagKafkaInfo> out) throws Exception {
            BigDecimal value = elements.iterator().next();
            TagKafkaInfo tagKafkaInfo = new TagKafkaInfo();
            tagKafkaInfo.setTime(sdf.format(context.window().getEnd()));
            tagKafkaInfo.setValue(value);
            tagKafkaInfo.setName(key.f0);
            tagKafkaInfo.setTopic(key.f1);
            tagKafkaInfo.setBytName(key.f2);
            tagKafkaInfo.setCalculateParam(key.f3);
            tagKafkaInfo.setCalculateType(key.f4);
            tagKafkaInfo.setLineId(key.f5);
            tagKafkaInfo.setTaskName(key.f6);
            tagKafkaInfo.setIsNormal(1);
            out.collect(tagKafkaInfo);
        }
    }

    class AvgAgg implements AggregateFunction<TagKafkaInfo, Tuple2<BigDecimal, BigDecimal>, BigDecimal> {

        @Override
        public Tuple2<BigDecimal, BigDecimal> createAccumulator() {
            return Tuple2.of(BigDecimal.valueOf(0L), BigDecimal.valueOf(0L));
        }

        @Override
        public Tuple2<BigDecimal, BigDecimal> add(TagKafkaInfo value, Tuple2<BigDecimal, BigDecimal> accumulator) {
            accumulator.f0 = accumulator.f0.add(value.getValue());
            accumulator.f1 = accumulator.f1.add(BigDecimal.valueOf(1L));
            return accumulator;
        }

        @Override
        public BigDecimal getResult(Tuple2<BigDecimal, BigDecimal> accumulator) {
            BigDecimal bigDecimal = null;
            try {
                bigDecimal = accumulator.f0.divide(accumulator.f1, 4, BigDecimal.ROUND_HALF_UP);
            } catch (Exception e){
                System.out.println("AVG 计算异常～～～～");
            }
            return bigDecimal;
        }

        @Override
        public Tuple2<BigDecimal, BigDecimal> merge(Tuple2<BigDecimal, BigDecimal> a, Tuple2<BigDecimal, BigDecimal> b) {
            return Tuple2.of(
                    a.f0.add(b.f0),
                    a.f1.add(b.f1)
            );
        }
    }


}
