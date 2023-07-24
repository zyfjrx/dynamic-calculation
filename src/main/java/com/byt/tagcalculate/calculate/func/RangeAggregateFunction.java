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
public class RangeAggregateFunction implements AggregateFunction<TagKafkaInfo, Tuple3<TagKafkaInfo, BigDecimal, BigDecimal>, TagKafkaInfo> {
    @Override
    public Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> createAccumulator() {
        // max - min
        return Tuple3.of(new TagKafkaInfo(), BigDecimal.valueOf(Long.MIN_VALUE), BigDecimal.valueOf(Long.MAX_VALUE));
    }

    @Override
    public Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> add(TagKafkaInfo tagKafkaInfo, Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> acc) {
        acc.f0 = tagKafkaInfo;
        acc.f1 = tagKafkaInfo.getValue().compareTo(acc.f1) > 0 ? tagKafkaInfo.getValue() : acc.f1;
        acc.f2 = tagKafkaInfo.getValue().compareTo(acc.f2) == -1 ? tagKafkaInfo.getValue() : acc.f2;
        return acc;
    }

    @Override
    public TagKafkaInfo getResult(Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> accumulator) {
        BigDecimal value = null;
        try {
            value = accumulator.f1.subtract(accumulator.f2);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("RANGE计算异常");
        }
        accumulator.f0.setValue(value);
        return accumulator.f0;
    }

    @Override
    public Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> merge(Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> acc1, Tuple3<TagKafkaInfo, BigDecimal, BigDecimal> acc2) {
        return Tuple3.of(
                acc1.f0,
                acc1.f1.compareTo(acc2.f1) > 0 ? acc1.f1 : acc2.f1,
                acc1.f2.compareTo(acc2.f2) == -1 ? acc1.f2 : acc2.f2
        );
    }
}
