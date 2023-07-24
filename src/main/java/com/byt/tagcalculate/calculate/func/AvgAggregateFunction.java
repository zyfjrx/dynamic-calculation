package com.byt.tagcalculate.calculate.func;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.math.BigDecimal;

/**
 * @title: 增量聚合
 * @author: zhangyf
 * @date: 2023/7/24 13:54
 **/
public class AvgAggregateFunction implements AggregateFunction<TagKafkaInfo, Tuple3<TagKafkaInfo, BigDecimal, BigDecimal>, TagKafkaInfo> {
    @Override
    public Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> createAccumulator() {
        return Tuple3.of(new TagKafkaInfo(), BigDecimal.ZERO, BigDecimal.ZERO);
    }

    @Override
    public Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> add(TagKafkaInfo tagKafkaInfo, Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> accumulator) {
        accumulator.f0 = tagKafkaInfo;
        accumulator.f1 = accumulator.f1.add(tagKafkaInfo.getValue());
        accumulator.f2 = accumulator.f2.add(BigDecimal.ONE);
        return accumulator;
    }

    @Override
    public TagKafkaInfo getResult(Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> accumulator) {
        BigDecimal value = null;
        try {
            value = accumulator.f1.divide(accumulator.f2, 4, BigDecimal.ROUND_HALF_UP);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("AVG计算异常");
        }
        accumulator.f0.setValue(value);
        return accumulator.f0;
    }

    @Override
    public Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> merge(Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> acc1, Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> acc2) {
        return Tuple3.of(
                acc1.f0,
                acc1.f1.add(acc2.f1),
                acc1.f2.add(acc2.f2)
        );
    }
}
