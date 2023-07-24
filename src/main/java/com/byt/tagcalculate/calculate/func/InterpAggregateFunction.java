package com.byt.tagcalculate.calculate.func;

import com.byt.tagcalculate.pojo.TagKafkaInfo;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @title: 增量聚合 - 最接近计算时刻的数据点取值
 * @author: zhangyf
 * @date: 2023/7/24 13:54
 **/
public class InterpAggregateFunction implements AggregateFunction<TagKafkaInfo, TagKafkaInfo, TagKafkaInfo> {


    @Override
    public TagKafkaInfo createAccumulator() {
        return new TagKafkaInfo();
    }

    @Override
    public TagKafkaInfo add(TagKafkaInfo tagKafkaInfo, TagKafkaInfo acc) {
        acc = tagKafkaInfo;
        return acc;
    }

    @Override
    public TagKafkaInfo getResult(TagKafkaInfo tagKafkaInfo) {
        return tagKafkaInfo;
    }

    @Override
    public TagKafkaInfo merge(TagKafkaInfo acc1, TagKafkaInfo acc2) {
        return acc1.getTimestamp() > acc2.getTimestamp() ? acc1 : acc2;
    }
}
