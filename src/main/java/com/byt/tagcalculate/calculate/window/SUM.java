package com.byt.tagcalculate.calculate.window;

import com.byt.tagcalculate.calculate.calculatechain.*;
import com.byt.tagcalculate.constants.PropertiesConstants;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import com.byt.common.utils.ConfigManager;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
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
 * @title: sum算子逻辑
 * @author: zhangyifan
 * @date: 2022/7/4 16:58
 */
public class SUM implements Transform {
    private String size;
    private String slide;

    public SUM(List<String> params) {
        this.size = params.get(0);
        this.slide = params.get(1);
    }

    public DataStream<TagKafkaInfo> sum(DataStream<TagKafkaInfo> in, String size, String slide) {
        return in
                .keyBy(new KeySelector<TagKafkaInfo, Tuple7<String, String, String, String, String, Integer, String>>() {
                    @Override
                    public Tuple7<String, String, String, String, String, Integer, String> getKey(TagKafkaInfo r) throws Exception {
                        return Tuple7.of(r.getName(), r.getTopic(), r.getBytName(), r.getCalculateParam(), r.getCalculateType(), r.getLineId(), r.getTaskName());
                    }
                })
                .window(WindowTimeSelector.getWindowTime(TimeParams.timeParams(size), TimeParams.timeParams(slide)))
                .aggregate(
                        new SumAgg(),
                        new SumResult()
                )
                .setParallelism(ConfigManager.getInteger(PropertiesConstants.STREAM_CALCULATE_PARALLELISM))
                .name("SUM");
    }

    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = sum(tStream.stream, size, slide);
        chain.doTransform(tStream);
    }

    public class SumResult extends ProcessWindowFunction<BigDecimal, TagKafkaInfo, Tuple7<String, String, String, String, String, Integer, String>, TimeWindow> {
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

    public class SumAgg implements AggregateFunction<TagKafkaInfo, BigDecimal, BigDecimal> {

        @Override
        public BigDecimal createAccumulator() {
            return BigDecimal.valueOf(0L);
        }

        @Override
        public BigDecimal add(TagKafkaInfo value, BigDecimal accumulator) {
            return accumulator.add(value.getValue());
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
}
