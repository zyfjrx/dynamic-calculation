package com.byt.calculate.window;

import com.byt.calculate.*;
import com.byt.constants.PropertiesConstants;
import com.byt.pojo.TagKafkaInfo;
import com.byt.utils.ConfigManager;
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
 * @title: 判断是否正数序列，有负数输出0，全部正数输出1
 * @author: zhangyifan
 * @date: 2022/9/5 09:35
 */
public class PSEQ implements Transform {
    private String size;
    private String slide;

    public PSEQ(List<String> params) {
        this.size = params.get(0);
        this.slide = params.get(1);
    }

    public DataStream<TagKafkaInfo> pSeq(DataStream<TagKafkaInfo> in, String size, String slide) {
        return in
                .keyBy(new KeySelector<TagKafkaInfo, Tuple7<String, String, String, String, String, Integer, String>>() {
                    @Override
                    public Tuple7<String, String, String, String, String, Integer, String> getKey(TagKafkaInfo r) throws Exception {
                        return Tuple7.of(r.getName(), r.getTopic(), r.getBytName(), r.getCalculateParam(), r.getCalculateType(), r.getLineId(), r.getTaskName());
                    }
                })
                .window(WindowTimeSelector.getWindowTime(TimeParams.timeParams(size),TimeParams.timeParams(slide)))
                .aggregate(
                        new PseqAgg(),
                        new PseqResult()
                )
                .setParallelism(ConfigManager.getInteger(PropertiesConstants.STREAM_CALCULATE_PARALLELISM))
                .name("PSEQ-正数序列检验-window("+size+","+slide+")");
    }

    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = pSeq(tStream.stream, size, slide);
        chain.doTransform(tStream);
    }


    private class PseqAgg implements AggregateFunction<TagKafkaInfo, BigDecimal, BigDecimal> {

        @Override
        public BigDecimal createAccumulator() {
            return BigDecimal.ZERO;
        }

        @Override
        public BigDecimal add(TagKafkaInfo tagKafkaInfo, BigDecimal accumulator) {
            if (tagKafkaInfo.getValue().compareTo(accumulator) == 1) {
                return accumulator;
            } else {
                return accumulator.add(BigDecimal.ONE);
            }
        }

        @Override
        public BigDecimal getResult(BigDecimal accumulator) {
            if (accumulator.compareTo(BigDecimal.ZERO) == 0) {
                return BigDecimal.ONE;
            } else {
                return BigDecimal.ZERO;
            }
        }

        @Override
        public BigDecimal merge(BigDecimal a, BigDecimal b) {
            return a.add(b);
        }
    }

    private class PseqResult extends ProcessWindowFunction<BigDecimal, TagKafkaInfo, Tuple7<String,String, String, String, String, Integer, String>, TimeWindow> {

        private SimpleDateFormat sdf;

        @Override
        public void open(Configuration parameters) throws Exception {
            sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        }

        @Override
        public void process(Tuple7<String,String, String, String, String, Integer, String> key, ProcessWindowFunction<BigDecimal, TagKafkaInfo, Tuple7<String,String, String, String, String, Integer, String>, TimeWindow>.Context context, Iterable<BigDecimal> elements, Collector<TagKafkaInfo> out) throws Exception {
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
