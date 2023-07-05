package com.byt.calculate.window;

import com.byt.calculate.TStream;
import com.byt.calculate.Transform;
import com.byt.calculate.TransformChain;
import com.byt.pojo.TagKafkaInfo;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * @title: EMA指标计算（移动平均）
 * @author: zhangyf
 * @date: 2023/3/15 9:01
 **/
public class EMA implements Transform {
    private Integer n;
    public EMA(List<String> params) {
        this.n = Integer.parseInt(params.get(0));
    }

    public DataStream<TagKafkaInfo> ema(DataStream<TagKafkaInfo> in,Integer n){
        return in
                .keyBy(new KeySelector<TagKafkaInfo, Tuple7<String, String, String, String, String, Integer, String>>() {
                    @Override
                    public Tuple7<String, String, String, String, String, Integer, String> getKey(TagKafkaInfo r) throws Exception {
                        return Tuple7.of(r.getName(), r.getTopic(), r.getBytName(), r.getCalculateParam(), r.getCalculateType(), r.getLineId(), r.getTaskName());
                    }
                })
                .process(new KeyedProcessFunction<Tuple7<String, String, String, String, String, Integer, String>, TagKafkaInfo, TagKafkaInfo>() {
                    private ValueState<BigDecimal> emaState;
                    private BigDecimal EMA;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        emaState = getRuntimeContext().getState(
                                new ValueStateDescriptor<BigDecimal>("ema",BigDecimal.class)
                        );
                    }

                    @Override
                    public void processElement(TagKafkaInfo tagKafkaInfo, KeyedProcessFunction<Tuple7<String, String, String, String, String, Integer, String>, TagKafkaInfo, TagKafkaInfo>.Context context, Collector<TagKafkaInfo> collector) throws Exception {
                        BigDecimal lastEma = emaState.value();
                        BigDecimal value = tagKafkaInfo.getValue();
                        if (lastEma == null){
                            emaState.update(value);
                        }else {
                            EMA = (value.multiply(BigDecimal.valueOf(2)).add(lastEma.multiply(BigDecimal.valueOf(n-1)))).divide(BigDecimal.valueOf(n+1),8, RoundingMode.HALF_UP);
                            emaState.update(EMA);
                        }
                        tagKafkaInfo.setValue(emaState.value());
                        collector.collect(tagKafkaInfo);
                    }
                });
    }

    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = ema(tStream.stream,n);
        chain.doTransform(tStream);
    }
}
