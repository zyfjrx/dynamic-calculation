package com.byt.tagcalculate.calculate.func;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.math.BigDecimal;

/**
 * @title: 增量聚合
 * @author: zhangyf
 * @date: 2023/7/24 13:54
 **/
public class SumAggregateFunction implements AggregateFunction<TagKafkaInfo, Tuple2<TagKafkaInfo, BigDecimal>, TagKafkaInfo> {

    @Override
    public Tuple2<TagKafkaInfo, BigDecimal> createAccumulator() {
        return Tuple2.of(new TagKafkaInfo(), BigDecimal.ZERO);
    }

    @Override
    public Tuple2<TagKafkaInfo, BigDecimal> add(TagKafkaInfo tagKafkaInfo, Tuple2<TagKafkaInfo, BigDecimal> acc) {
        acc.f0 = tagKafkaInfo;
        acc.f1 = tagKafkaInfo.getValue().add(acc.f1);
        return acc;
    }

    @Override
    public TagKafkaInfo getResult(Tuple2<TagKafkaInfo, BigDecimal> acc) {
        acc.f0.setValue(acc.f1);
        return acc.f0;
    }

    @Override
    public Tuple2<TagKafkaInfo, BigDecimal> merge(Tuple2<TagKafkaInfo, BigDecimal> acc1, Tuple2<TagKafkaInfo, BigDecimal> acc2) {
        return Tuple2.of(
                acc1.f0,
                acc1.f1.add(acc2.f1)
        );
    }
}
