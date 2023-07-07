package com.byt.tagcalculate.func;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

/**
 * @title: 一阶低通滤波算法
 * @author: zhangyifan
 * @date: 2022/8/28 13:47
 */
public class FirstOrderFilterFunction extends KeyedProcessFunction<Tuple3<String,String,Integer>,TagKafkaInfo, TagKafkaInfo> {
    private BigDecimal a;
    private ValueState<BigDecimal> lastFirstOrder;
    private SimpleDateFormat sdf;

    public FirstOrderFilterFunction(BigDecimal a) {
        this.a = a;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        lastFirstOrder = getRuntimeContext().getState(
                new ValueStateDescriptor<BigDecimal>(
                        "firstOrder",
                        Types.BIG_DEC
                )
        );
        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    }

    @Override
    public void processElement(TagKafkaInfo tagKafkaInfo, KeyedProcessFunction<Tuple3<String,String, Integer>, TagKafkaInfo, TagKafkaInfo>.Context ctx, Collector<TagKafkaInfo> out) throws Exception {
        if (lastFirstOrder.value() == null) {
            lastFirstOrder.update(tagKafkaInfo.getValue());
        }
        lastFirstOrder.update(
                a.multiply(tagKafkaInfo.getValue())
                        .add(
                                BigDecimal.ONE.subtract(a).multiply(lastFirstOrder.value())
                        )
        );
        tagKafkaInfo.setValue(lastFirstOrder.value().setScale(4, BigDecimal.ROUND_HALF_UP));
        // TODO dev
        //tagKafkaInfo.setTime(sdf.format(new Timestamp(System.currentTimeMillis())));
        out.collect(tagKafkaInfo);
    }
}
