package com.byt.tagcalculate.calculate.window;

import com.byt.tagcalculate.calculate.calculatechain.TStream;
import com.byt.tagcalculate.calculate.calculatechain.TimeParams;
import com.byt.tagcalculate.calculate.calculatechain.Transform;
import com.byt.tagcalculate.calculate.calculatechain.TransformChain;
import com.byt.tagcalculate.constants.PropertiesConstants;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.common.utils.ConfigManager;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * @title: 最大值算子
 * @author: zhangyifan
 * @date: 2022/8/18 10:08
 */
public class MIN implements Transform {
    private String size;
    private String slide;

    public MIN(List<String> params) {
        this.size = params.get(0);
        this.slide = params.get(1);
    }

    public DataStream<TagKafkaInfo> min(DataStream<TagKafkaInfo> in, String size, String slide) {
        return in
                .keyBy(new KeySelector<TagKafkaInfo, Tuple7<String, String, String, String, String, Integer, String>>() {
                    @Override
                    public Tuple7<String, String, String, String, String, Integer, String> getKey(TagKafkaInfo r) throws Exception {
                        return Tuple7.of(r.getName(), r.getTopic(), r.getBytName(), r.getCalculateParam(), r.getCalculateType(), r.getLineId(), r.getTaskName());
                    }
                })
                .window(SlidingProcessingTimeWindows.of(TimeParams.timeParams(size), TimeParams.timeParams(slide)))
                .aggregate(
                        new MinAgg(),
                        new MinResult()
                )
                .setParallelism(ConfigManager.getInteger(PropertiesConstants.STREAM_CALCULATE_PARALLELISM))
                .name("MIN-window(" + size + "," + slide + ")");
    }

    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = min(tStream.stream, size, slide);
        chain.doTransform(tStream);
    }

    private class MinAgg implements AggregateFunction<TagKafkaInfo, BigDecimal, BigDecimal> {
        @Override
        public BigDecimal createAccumulator() {
            return BigDecimal.valueOf(Long.MAX_VALUE);
        }

        @Override
        public BigDecimal add(TagKafkaInfo value, BigDecimal accumulator) {
            accumulator = value.getValue().compareTo(accumulator) == -1 ? value.getValue() : accumulator;
            return accumulator;
        }

        @Override
        public BigDecimal getResult(BigDecimal accumulator) {
            return accumulator;
        }

        @Override
        public BigDecimal merge(BigDecimal a, BigDecimal b) {
            return a.add(b);
        }
    }

    class MinResult extends ProcessWindowFunction<BigDecimal, TagKafkaInfo, Tuple7<String, String, String, String, String, Integer, String>, TimeWindow> {
        private SimpleDateFormat sdf;

        @Override
        public void open(Configuration parameters) throws Exception {
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }

        @Override
        public void process(Tuple7<String, String, String, String, String, Integer, String> key, ProcessWindowFunction<BigDecimal, TagKafkaInfo, Tuple7<String, String, String, String, String, Integer, String>, TimeWindow>.Context context, Iterable<BigDecimal> elements, Collector<TagKafkaInfo> out) throws Exception {
            BigDecimal value = elements.iterator().next();
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
            out.collect(tagKafkaInfo);
        }
    }
}
