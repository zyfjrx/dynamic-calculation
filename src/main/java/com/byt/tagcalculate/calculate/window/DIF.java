package com.byt.tagcalculate.calculate.window;

import com.byt.tagcalculate.calculate.calculatechain.TStream;
import com.byt.tagcalculate.calculate.calculatechain.Transform;
import com.byt.tagcalculate.calculate.calculatechain.TransformChain;
import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

/**
 * @title: DIF指标计算（计算离差值）
 *
 * @author: zhangyf
 * @date: 2023/3/15 9:01
 **/
public class DIF implements Transform {
    private Integer n1;
    private Integer n2;
    public DIF(List<String> params) {
        this.n1 = Integer.parseInt(params.get(0));
        this.n2 = Integer.parseInt(params.get(1));
    }

    public DataStream<TagKafkaInfo> ema(DataStream<TagKafkaInfo> in,Integer n1,Integer n2){
        return in
                .keyBy(new KeySelector<TagKafkaInfo, Tuple7<String, String, String, String, String, Integer, String>>() {
                    @Override
                    public Tuple7<String, String, String, String, String, Integer, String> getKey(TagKafkaInfo r) throws Exception {
                        return Tuple7.of(r.getName(), r.getTopic(), r.getBytName(), r.getCalculateParam(), r.getCalculateType(), r.getLineId(), r.getTaskName());
                    }
                })
                .process(new KeyedProcessFunction<Tuple7<String, String, String, String, String, Integer, String>, TagKafkaInfo, TagKafkaInfo>() {
                    private ValueState<Tuple2<BigDecimal,BigDecimal>> emaState;
                    private BigDecimal EMA1;
                    private BigDecimal EMA2;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        emaState = getRuntimeContext().getState(
                                new ValueStateDescriptor<Tuple2<BigDecimal,BigDecimal>>("ema",Types.TUPLE(Types.BIG_DEC))
                        );
                    }

                    @Override
                    public void processElement(TagKafkaInfo tagKafkaInfo, KeyedProcessFunction<Tuple7<String, String, String, String, String, Integer, String>, TagKafkaInfo, TagKafkaInfo>.Context context, Collector<TagKafkaInfo> collector) throws Exception {
                        Tuple2<BigDecimal, BigDecimal> lastState = emaState.value();
                        BigDecimal value = tagKafkaInfo.getValue();
                        if (lastState == null){
                            emaState.update(Tuple2.of(value,value));
                        }else {
                            EMA1 = (value.multiply(BigDecimal.valueOf(2)).add(lastState.f0.multiply(BigDecimal.valueOf(n1-1)))).divide(BigDecimal.valueOf(n1+1),9, RoundingMode.HALF_UP);
                            EMA2 = (value.multiply(BigDecimal.valueOf(2)).add(lastState.f1.multiply(BigDecimal.valueOf(n2-1)))).divide(BigDecimal.valueOf(n2+1),9, RoundingMode.HALF_UP);
                            emaState.update(Tuple2.of(EMA1,EMA2));

                        }
                        System.out.println("ema1:"+emaState.value().f0 +"-----"+"em2:"+emaState.value().f1);
                        tagKafkaInfo.setValue(emaState.value().f0.subtract(emaState.value().f1));
                        collector.collect(tagKafkaInfo);
                    }
                });
    }

    @Override
    public void doTransForm(TStream tStream, TransformChain chain) {
        tStream.stream = ema(tStream.stream,n1,n2);
        chain.doTransform(tStream);
    }
}
