package com.byt.tagcalculate.calculate.window;

import com.byt.tagcalculate.calculate.*;
import com.byt.tagcalculate.constants.PropertiesConstants;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.common.utils.ConfigManager;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * @title: 极差算子
 * @author: zhangyifan
 * @date: 2022/7/27 16:37
 */
public class RANGE implements Transform {
    private String size;
    private String slide;

    public RANGE(List<String> params) {
        this.size = params.get(0);
        this.slide = params.get(1);
    }


    public DataStream<TagKafkaInfo> range(DataStream<TagKafkaInfo> in, String size, String slide) {
        return in
                .keyBy(new KeySelector<TagKafkaInfo, Tuple7<String, String, String, String, String, Integer, String>>() {
                    @Override
                    public Tuple7<String, String, String, String, String, Integer, String> getKey(TagKafkaInfo r) throws Exception {
                        return Tuple7.of(r.getName(), r.getTopic(), r.getBytName(), r.getCalculateParam(), r.getCalculateType(), r.getLineId(), r.getTaskName());
                    }
                })
                .window(WindowTimeSelector.getWindowTime(TimeParams.timeParams(size), TimeParams.timeParams(slide)))
                .aggregate(
                        new RangeAgg(),
                        new RangeResult()
                )
                .setParallelism(ConfigManager.getInteger(PropertiesConstants.STREAM_CALCULATE_PARALLELISM))
                .name("RANGE-极差-window(" + size + "," + slide + ")");
    }

    class RangeAgg implements AggregateFunction<TagKafkaInfo, Tuple2<BigDecimal, BigDecimal>, BigDecimal> {

        @Override
        public Tuple2<BigDecimal, BigDecimal> createAccumulator() {
            return Tuple2.of(BigDecimal.valueOf(Long.MIN_VALUE), BigDecimal.valueOf(Long.MAX_VALUE));
        }

        @Override
        public Tuple2<BigDecimal, BigDecimal> add(TagKafkaInfo tagKafkaInfo, Tuple2<BigDecimal, BigDecimal> accumulator) {
            BigDecimal max = tagKafkaInfo.getValue().compareTo(accumulator.f0) == 1 ? tagKafkaInfo.getValue() : accumulator.f0;
            BigDecimal min = tagKafkaInfo.getValue().compareTo(accumulator.f1) == -1 ? tagKafkaInfo.getValue() : accumulator.f1;
            return Tuple2.of(max, min);
        }

        @Override
        public BigDecimal getResult(Tuple2<BigDecimal, BigDecimal> accumulator) {
            System.out.println("range:"+accumulator.f0 +"==========="+ accumulator.f1);
            return accumulator.f0.subtract(accumulator.f1);
        }

        @Override
        public Tuple2<BigDecimal, BigDecimal> merge(Tuple2<BigDecimal, BigDecimal> acc1, Tuple2<BigDecimal, BigDecimal> acc2) {
            return Tuple2.of(
                    acc1.f0.add(acc2.f0),
                    acc1.f1.add(acc2.f1)
            );
        }
    }

    class RangeResult extends ProcessWindowFunction<BigDecimal, TagKafkaInfo, Tuple7<String, String, String, String, String, Integer, String>, TimeWindow> {
        private SimpleDateFormat sdf;

        @Override
        public void open(Configuration parameters) throws Exception {
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }


        @Override
        public void process(Tuple7<String, String, String, String, String, Integer, String> key, ProcessWindowFunction<BigDecimal, TagKafkaInfo, Tuple7<String,String, String, String, String, Integer, String>, TimeWindow>.Context context, Iterable<BigDecimal> iterable, Collector<TagKafkaInfo> collector) throws Exception {
            BigDecimal value = iterable.iterator().next();
            TagKafkaInfo tagKafkaInfo = new TagKafkaInfo();
            tagKafkaInfo.setName(key.f0);
            tagKafkaInfo.setTopic(key.f1);
            tagKafkaInfo.setBytName(key.f2);
            tagKafkaInfo.setCalculateParam(key.f3);
            tagKafkaInfo.setCalculateType(key.f4);
            tagKafkaInfo.setLineId(key.f5);
            tagKafkaInfo.setTaskName(key.f6);
            tagKafkaInfo.setValue(value);
            tagKafkaInfo.setTime(sdf.format(context.window().getEnd()));
            collector.collect(tagKafkaInfo);
        }
    }

    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = range(tStream.stream, size, slide);
        chain.doTransform(tStream);
    }
}
